//! Private ini storage for a device's durable refresh credential.
//!
//! The file may be shared with unrelated settings, so this is a line editor
//! rather than a parse-and-reserialise round trip: only `RankedRefreshToken`
//! and `RankedBaseUrl` inside `[Settings]` are ever touched, and every other
//! line, a leading BOM, the LF/CRLF style and the trailing-newline status are
//! kept. Anything ambiguous (a repeated section or key, a symlink, a
//! non-regular file, invalid UTF-8) is refused rather than guessed at, and a
//! file that failed to parse is never overwritten.
//!
//! Writes go through a sibling temporary file created 0600 and renamed over
//! the destination, so a failure leaves the original intact. No error message
//! ever contains a stored value.

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result, bail};

const SECTION: &str = "[Settings]";
const REFRESH_KEY: &str = "RankedRefreshToken";
const BASE_KEY: &str = "RankedBaseUrl";
const BOM: char = '\u{feff}';

/// The durable device credential saved by a completed login.
#[derive(Clone, PartialEq, Eq)]
pub struct StoredDevice {
    /// Canonical origin the refresh credential was issued by.
    pub base: String,
    /// Opaque long-lived refresh credential; never printed.
    pub refresh: String,
}

impl std::fmt::Debug for StoredDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoredDevice")
            .field("base", &self.base)
            .field("refresh", &"<redacted>")
            .finish()
    }
}

/// The saved device credential, or `None` when the file is missing or holds
/// no refresh value.
pub fn load(file: &Path) -> Result<Option<StoredDevice>> {
    let Some(doc) = read_doc(file)? else {
        return Ok(None);
    };
    let settings = settings(&doc, file)?;
    let refresh = match settings.refresh.map(|i| entry_value(&doc.lines[i])) {
        Some(value) if !value.is_empty() => value,
        _ => return Ok(None),
    };
    let base = match settings.base.map(|i| entry_value(&doc.lines[i])) {
        Some(value) if !value.is_empty() => value,
        _ => bail!("{} has {REFRESH_KEY} but no {BASE_KEY}", file.display()),
    };
    if !doc.private {
        bail!(
            "{0} holds a device credential but is readable by group/others; run chmod 600 {0}",
            file.display()
        );
    }
    Ok(Some(StoredDevice {
        base: base.to_string(),
        refresh: refresh.to_string(),
    }))
}

/// Store `refresh` and its issuing `base` in `[Settings]`, creating the file
/// (or the section) if needed. The result is always mode 0600 on Unix.
pub fn save(file: &Path, base: &str, refresh: &str) -> Result<()> {
    for value in [base, refresh] {
        if value.is_empty() || value.contains(['\r', '\n']) {
            bail!("refusing to save an empty or multi-line value");
        }
        // `load` trims values, so padding would not survive a round trip.
        if value.trim() != value {
            bail!("refusing to save a value with surrounding whitespace");
        }
    }
    let mut doc = read_doc(file)?.unwrap_or_else(|| parse(String::new(), true));
    let settings = settings(&doc, file)?;

    let mut missing = Vec::with_capacity(2);
    let refresh_line = format!("{REFRESH_KEY}={refresh}");
    match settings.refresh {
        Some(i) => doc.lines[i] = refresh_line,
        None => missing.push(refresh_line),
    }
    let base_line = format!("{BASE_KEY}={base}");
    match settings.base {
        Some(i) => doc.lines[i] = base_line,
        None => missing.push(base_line),
    }

    match settings.header {
        Some(header) => {
            // After the section's last non-blank line, so blank separators
            // before the next header stay where they were.
            let at = (header + 1..settings.end)
                .rev()
                .find(|&i| !doc.lines[i].trim().is_empty())
                .map_or(header + 1, |i| i + 1);
            doc.lines.splice(at..at, missing);
        }
        None => {
            if doc.lines.is_empty() {
                // Nothing to preserve: shape it like a fresh file.
                doc.trailing_newline = true;
            } else if doc.lines.last().is_some_and(|l| !l.trim().is_empty()) {
                doc.lines.push(String::new());
            }
            doc.lines.push(SECTION.to_string());
            doc.lines.extend(missing);
        }
    }
    write_atomic(file, render(&doc).as_bytes())
}

/// Empty the stored refresh value, keeping every other line (including
/// `RankedBaseUrl`). A missing file or absent key is already clear.
pub fn clear(file: &Path) -> Result<()> {
    let Some(mut doc) = read_doc(file)? else {
        return Ok(());
    };
    let Some(i) = settings(&doc, file)?.refresh else {
        return Ok(());
    };
    doc.lines[i] = format!("{REFRESH_KEY}=");
    write_atomic(file, render(&doc).as_bytes())
}

/// A file split into lines with its framing recorded, so it renders back
/// unchanged apart from edited lines. A file mixing terminators is rendered
/// in its detected style.
struct Doc {
    bom: bool,
    lines: Vec<String>,
    crlf: bool,
    trailing_newline: bool,
    /// No group/other permission bits are set (always true off Unix).
    private: bool,
}

/// Where `[Settings]` and its target keys sit in a [`Doc`].
struct Settings {
    header: Option<usize>,
    /// One past the section's last line (the next header, or end of file).
    end: usize,
    refresh: Option<usize>,
    base: Option<usize>,
}

/// Read and split `file`; `None` when it does not exist.
fn read_doc(file: &Path) -> Result<Option<Doc>> {
    let meta = match fs::symlink_metadata(file) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("inspecting {}", file.display())),
    };
    if meta.file_type().is_symlink() {
        bail!("refusing to use {}: it is a symlink", file.display());
    }
    if !meta.is_file() {
        bail!("refusing to use {}: not a regular file", file.display());
    }
    let mut handle = File::open(file).with_context(|| format!("opening {}", file.display()))?;
    let opened = handle
        .metadata()
        .with_context(|| format!("inspecting {}", file.display()))?;
    if !same_file(&meta, &opened) {
        bail!(
            "refusing to use {}: it changed while being opened",
            file.display()
        );
    }
    let mut bytes = Vec::with_capacity(usize::try_from(opened.len()).unwrap_or(0));
    handle
        .read_to_end(&mut bytes)
        .with_context(|| format!("reading {}", file.display()))?;
    let text = String::from_utf8(bytes)
        .ok()
        .with_context(|| format!("{} is not valid UTF-8", file.display()))?;
    Ok(Some(parse(text, is_private(&opened))))
}

/// The opened handle is the regular file that was checked by path, not
/// something (such as a symlink) swapped in between the two steps.
#[cfg(unix)]
fn same_file(checked: &fs::Metadata, opened: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    opened.is_file() && checked.dev() == opened.dev() && checked.ino() == opened.ino()
}

#[cfg(not(unix))]
fn same_file(_checked: &fs::Metadata, opened: &fs::Metadata) -> bool {
    opened.is_file()
}

#[cfg(unix)]
fn is_private(meta: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o077 == 0
}

#[cfg(not(unix))]
fn is_private(_meta: &fs::Metadata) -> bool {
    true
}

fn parse(text: String, private: bool) -> Doc {
    let body = text.strip_prefix(BOM);
    let bom = body.is_some();
    let body = body.unwrap_or(&text);
    let mut lines = Vec::new();
    let mut rest = body;
    while let Some((line, tail)) = rest.split_once('\n') {
        lines.push(line.strip_suffix('\r').unwrap_or(line).to_string());
        rest = tail;
    }
    // An unterminated last line (a lone `\r` included) is content, kept as is.
    if !rest.is_empty() {
        lines.push(rest.to_string());
    }
    Doc {
        bom,
        lines,
        crlf: body.contains("\r\n"),
        trailing_newline: body.ends_with('\n'),
        private,
    }
}

fn render(doc: &Doc) -> String {
    let eol = if doc.crlf { "\r\n" } else { "\n" };
    let mut out = String::with_capacity(
        BOM.len_utf8() + doc.lines.iter().map(|l| l.len() + eol.len()).sum::<usize>(),
    );
    if doc.bom {
        out.push(BOM);
    }
    for (i, line) in doc.lines.iter().enumerate() {
        if i > 0 {
            out.push_str(eol);
        }
        out.push_str(line);
    }
    if doc.trailing_newline && !doc.lines.is_empty() {
        out.push_str(eol);
    }
    out
}

fn is_comment(line: &str) -> bool {
    let line = line.trim_start();
    line.starts_with(';') || line.starts_with('#')
}

fn is_header(line: &str) -> bool {
    let line = line.trim();
    line.starts_with('[') && line.ends_with(']')
}

/// The trimmed key of a `key=value` line, if it is one.
fn entry_key(line: &str) -> Option<&str> {
    if is_comment(line) || is_header(line) {
        return None;
    }
    line.split_once('=').map(|(key, _)| key.trim())
}

/// The trimmed value of a line already known to be an entry.
fn entry_value(line: &str) -> &str {
    line.split_once('=').map_or("", |(_, value)| value.trim())
}

/// Locate `[Settings]` and its target keys, refusing duplicates of either.
fn settings(doc: &Doc, file: &Path) -> Result<Settings> {
    let lines = &doc.lines;
    let header = lines.iter().position(|l| l.trim() == SECTION);
    let mut found = Settings {
        header,
        end: lines.len(),
        refresh: None,
        base: None,
    };
    let Some(header) = header else {
        return Ok(found);
    };
    let after = &lines[header + 1..];
    if after.iter().any(|l| l.trim() == SECTION) {
        bail!("{} has more than one {SECTION} section", file.display());
    }
    found.end = after
        .iter()
        .position(|l| is_header(l))
        .map_or(lines.len(), |i| header + 1 + i);
    for (i, line) in lines.iter().enumerate().take(found.end).skip(header + 1) {
        let (slot, key) = match entry_key(line) {
            Some(REFRESH_KEY) => (&mut found.refresh, REFRESH_KEY),
            Some(BASE_KEY) => (&mut found.base, BASE_KEY),
            _ => continue,
        };
        if slot.replace(i).is_some() {
            bail!("{} repeats {key} in {SECTION}", file.display());
        }
    }
    Ok(found)
}

/// Replace `file` with `bytes` via a 0600 sibling temporary file, removing
/// only that temporary file if anything fails.
fn write_atomic(file: &Path, bytes: &[u8]) -> Result<()> {
    let name = file
        .file_name()
        .with_context(|| format!("{} does not name a file", file.display()))?;
    let dir = match file.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    let mut temp_name = OsString::from(".");
    temp_name.push(name);
    temp_name.push(format!(".{}.tmp", uuid::Uuid::new_v4()));
    let temp = dir.join(temp_name);

    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let handle = options
        .open(&temp)
        .with_context(|| format!("creating a temporary file beside {}", file.display()))?;

    if let Err(e) = fill_and_rename(handle, bytes, &temp, file) {
        let _ = fs::remove_file(&temp);
        return Err(e).with_context(|| format!("writing {}", file.display()));
    }
    // Make the rename itself durable. The data is already in place, so a
    // directory that cannot be synced is not a failed write.
    if let Ok(dir) = File::open(dir) {
        let _ = dir.sync_all();
    }
    Ok(())
}

/// Write the freshly created temporary file and move it over `file`.
fn fill_and_rename(
    mut handle: File,
    bytes: &[u8],
    temp: &Path,
    file: &Path,
) -> std::io::Result<()> {
    // The creation mode is filtered by the umask; pin it exactly before any
    // byte is written.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        handle.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    handle.write_all(bytes)?;
    handle.sync_all()?;
    drop(handle);
    fs::rename(temp, file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    const BASE: &str = "https://ranked.example.com";
    /// A canary, never a real credential: no error may echo it back.
    const REFRESH: &str = "CANARY-REFRESH-VALUE";

    /// A unique directory in the system temp dir, removed when the test ends.
    /// Fixtures are written 0600, the mode a real credential file has.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("umvc3-ini-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(&dir).expect("create the scratch directory");
            Self(dir)
        }

        fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }

        fn write(&self, name: &str, bytes: &[u8]) -> PathBuf {
            let path = self.path(name);
            fs::write(&path, bytes).expect("write the fixture");
            set_mode(&path, 0o600);
            path
        }

        /// The names in the directory, sorted: proof no temp file survived.
        fn names(&self) -> Vec<String> {
            let mut names: Vec<String> = fs::read_dir(&self.0)
                .expect("list the scratch directory")
                .map(|entry| {
                    entry
                        .expect("directory entry")
                        .file_name()
                        .to_string_lossy()
                        .into_owned()
                })
                .collect();
            names.sort();
            names
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn text(path: &Path) -> String {
        fs::read_to_string(path).expect("read the fixture back")
    }

    fn message(error: &anyhow::Error) -> String {
        format!("{error:#}")
    }

    #[cfg(unix)]
    fn set_mode(path: &Path, bits: u32) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(bits)).expect("set the mode");
    }

    #[cfg(not(unix))]
    fn set_mode(_path: &Path, _bits: u32) {}

    #[cfg(unix)]
    fn mode(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        fs::metadata(path).expect("stat").permissions().mode() & 0o777
    }

    /// A `[Settings]` block holding a stored credential.
    fn settings_with_credential() -> String {
        format!("[Settings]\nRankedRefreshToken={REFRESH}\nRankedBaseUrl={BASE}\n")
    }

    #[test]
    fn save_edits_only_its_own_keys_in_place() {
        let scratch = Scratch::new();
        let file = scratch.write(
            "config.ini",
            b"[Game]\r\n; a comment worth keeping\r\nVolume=7\r\n\r\n[Settings]\r\n\
              Fullscreen=1\r\nRankedRefreshToken=old\r\n\r\n[Other]\r\n\
              RankedRefreshToken=untouched\r\nRankedBaseUrl=untouched\r\n",
        );

        save(&file, BASE, REFRESH).expect("save");

        assert_eq!(
            text(&file),
            "[Game]\r\n; a comment worth keeping\r\nVolume=7\r\n\r\n[Settings]\r\n\
             Fullscreen=1\r\nRankedRefreshToken=CANARY-REFRESH-VALUE\r\n\
             RankedBaseUrl=https://ranked.example.com\r\n\r\n[Other]\r\n\
             RankedRefreshToken=untouched\r\nRankedBaseUrl=untouched\r\n",
            "only the two Settings keys change, and the CRLF framing survives"
        );
        assert_eq!(
            load(&file).expect("load"),
            Some(StoredDevice {
                base: BASE.to_string(),
                refresh: REFRESH.to_string(),
            })
        );
    }

    #[test]
    fn save_keeps_a_bom_the_lf_style_and_the_missing_final_newline() {
        let scratch = Scratch::new();
        let file = scratch.write(
            "bom.ini",
            "\u{feff}[Settings]\nRankedRefreshToken=old".as_bytes(),
        );

        save(&file, BASE, REFRESH).expect("save");

        assert_eq!(
            text(&file),
            "\u{feff}[Settings]\nRankedRefreshToken=CANARY-REFRESH-VALUE\n\
             RankedBaseUrl=https://ranked.example.com"
        );
    }

    #[test]
    fn save_creates_a_section_and_a_0600_file() {
        let scratch = Scratch::new();
        let file = scratch.path("new.ini");
        assert!(load(&file).expect("load a missing file").is_none());

        save(&file, BASE, REFRESH).expect("save");

        assert_eq!(
            text(&file),
            "[Settings]\nRankedRefreshToken=CANARY-REFRESH-VALUE\n\
             RankedBaseUrl=https://ranked.example.com\n"
        );
        #[cfg(unix)]
        assert_eq!(mode(&file), 0o600, "a new credential file is private");
        assert_eq!(
            scratch.names(),
            vec!["new.ini".to_string()],
            "the temporary file is renamed, not left behind"
        );
    }

    #[cfg(unix)]
    #[test]
    fn save_tightens_the_mode_of_an_existing_file() {
        let scratch = Scratch::new();
        let file = scratch.write("shared.ini", b"[Settings]\nNote=hi\n");
        set_mode(&file, 0o644);

        save(&file, BASE, REFRESH).expect("save");

        assert_eq!(mode(&file), 0o600);
    }

    #[cfg(unix)]
    #[test]
    fn a_group_or_world_readable_credential_is_refused_untouched() {
        let scratch = Scratch::new();
        for bits in [0o604, 0o640, 0o644, 0o660, 0o607] {
            let name = format!("mode-{bits:o}.ini");
            let file = scratch.write(&name, settings_with_credential().as_bytes());
            set_mode(&file, bits);

            let error = load(&file).expect_err("a readable credential must be refused");
            assert!(
                message(&error).contains("chmod 600"),
                "{name}: {error:#} should say how to fix it"
            );
            assert!(
                !message(&error).contains(REFRESH),
                "{name}: the refusal must not echo the credential"
            );
            assert_eq!(mode(&file), bits, "{name}: load must not quietly chmod");
        }

        // Nothing secret inside, so an ordinary shared settings file is fine.
        let plain = scratch.write("plain.ini", b"[Settings]\nNote=hi\nRankedBaseUrl=x\n");
        set_mode(&plain, 0o644);
        assert!(load(&plain).expect("load").is_none());
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_is_refused_and_its_target_is_never_touched() {
        let scratch = Scratch::new();
        let target = scratch.write("real.ini", settings_with_credential().as_bytes());
        let before = text(&target);
        let link = scratch.path("link.ini");
        std::os::unix::fs::symlink(&target, &link).expect("symlink");

        for error in [
            load(&link).expect_err("load"),
            save(&link, BASE, "OTHER-VALUE").expect_err("save"),
            clear(&link).expect_err("clear"),
        ] {
            assert!(
                message(&error).contains("symlink"),
                "{error:#} should refuse the symlink outright"
            );
        }
        assert_eq!(text(&target), before, "the target is untouched");
        assert!(
            fs::symlink_metadata(&link)
                .expect("stat the link")
                .file_type()
                .is_symlink(),
            "the link is still a link"
        );
    }

    #[test]
    fn a_file_that_cannot_be_parsed_is_never_rewritten() {
        let scratch = Scratch::new();
        let cases: &[(&str, &[u8], &str)] = &[
            (
                "duplicate refresh",
                b"[Settings]\nRankedRefreshToken=a\nRankedRefreshToken=b\nRankedBaseUrl=x\n",
                "repeats RankedRefreshToken",
            ),
            (
                "duplicate base",
                b"[Settings]\nRankedBaseUrl=x\nRankedBaseUrl=y\n",
                "repeats RankedBaseUrl",
            ),
            (
                "duplicate section",
                b"[Settings]\nRankedBaseUrl=x\n[Other]\n[Settings]\n",
                "more than one [Settings] section",
            ),
            (
                "invalid utf-8",
                b"[Settings]\nRankedRefreshToken=\xff\xfe\n",
                "not valid UTF-8",
            ),
        ];
        for (name, bytes, expected) in cases {
            let file = scratch.write("bad.ini", bytes);
            assert!(
                message(&load(&file).expect_err("load")).contains(expected),
                "{name}: load should refuse it"
            );
            save(&file, BASE, REFRESH).expect_err("save must refuse it too");
            clear(&file).expect_err("clear must refuse it too");
            assert_eq!(
                fs::read(&file).expect("read"),
                *bytes,
                "{name}: bytes survive"
            );
            assert_eq!(
                scratch.names(),
                vec!["bad.ini".to_string()],
                "{name}: no temp file left behind"
            );
        }
    }

    #[test]
    fn a_repeated_key_outside_settings_is_ignored() {
        let scratch = Scratch::new();
        let file = scratch.write(
            "other.ini",
            b"[Other]\nRankedRefreshToken=a\nRankedRefreshToken=b\n\n[Settings]\n\
              RankedRefreshToken=R\nRankedBaseUrl=https://x.example.com\n",
        );

        assert_eq!(
            load(&file).expect("load"),
            Some(StoredDevice {
                base: "https://x.example.com".to_string(),
                refresh: "R".to_string(),
            })
        );
    }

    #[test]
    fn save_refuses_values_that_would_not_round_trip() {
        let scratch = Scratch::new();
        let file = scratch.write("ok.ini", b"[Settings]\nNote=hi\n");
        let before = text(&file);

        for (base, refresh) in [
            ("", REFRESH),
            (BASE, ""),
            (BASE, "one\ntwo"),
            (BASE, "carriage\rreturn"),
            (BASE, "padded "),
            (" padded", REFRESH),
        ] {
            let error = save(&file, base, refresh).expect_err("must be refused");
            assert!(
                message(&error).contains("refusing to save"),
                "{base:?}/{refresh:?}: {error:#}"
            );
        }
        assert_eq!(text(&file), before);

        let missing = scratch.path("never-created.ini");
        save(&missing, "", REFRESH).expect_err("must be refused");
        assert!(!missing.exists(), "a refused save creates nothing");
    }

    #[test]
    fn clear_empties_only_the_refresh_value() {
        let scratch = Scratch::new();
        let file = scratch.write(
            "clear.ini",
            b"[Game]\nVolume=7\n\n[Settings]\nRankedRefreshToken=R\n\
              RankedBaseUrl=https://x.example.com\nFullscreen=1\n",
        );
        let cleared = "[Game]\nVolume=7\n\n[Settings]\nRankedRefreshToken=\n\
                       RankedBaseUrl=https://x.example.com\nFullscreen=1\n";

        clear(&file).expect("clear");

        assert_eq!(text(&file), cleared, "the base and every other line stay");
        assert!(load(&file).expect("load").is_none());

        clear(&file).expect("clearing again is a no-op");
        assert_eq!(text(&file), cleared);

        let saved = scratch.path("saved.ini");
        save(&saved, BASE, REFRESH).expect("save");
        clear(&saved).expect("clear");
        assert!(load(&saved).expect("load").is_none());
        #[cfg(unix)]
        assert_eq!(mode(&saved), 0o600, "clearing keeps the file private");

        let missing = scratch.path("missing.ini");
        clear(&missing).expect("a missing file is already clear");
        assert!(!missing.exists());

        let plain = scratch.write("no-key.ini", b"[Settings]\nNote=hi\n");
        let before = text(&plain);
        clear(&plain).expect("clear");
        assert_eq!(text(&plain), before);
    }

    #[test]
    fn load_reads_trimmed_values_and_only_inside_settings() {
        let scratch = Scratch::new();
        let cases: &[(&str, &[u8])] = &[
            (
                "another section",
                b"[Other]\nRankedRefreshToken=outside\nRankedBaseUrl=https://out.example.com\n",
            ),
            (
                "commented out",
                b"[Settings]\n;RankedRefreshToken=commented\n#RankedBaseUrl=commented\n",
            ),
            (
                "case-sensitive key",
                b"[Settings]\nrankedrefreshtoken=R3\nRankedBaseUrl=x\n",
            ),
            (
                "empty value",
                b"[Settings]\nRankedRefreshToken=\nRankedBaseUrl=x\n",
            ),
        ];
        for (name, bytes) in cases {
            let file = scratch.write("case.ini", bytes);
            assert!(
                load(&file).expect("load").is_none(),
                "{name} is not a saved session"
            );
        }

        let spaced = scratch.write(
            "spaced.ini",
            b"[Settings]\n  RankedRefreshToken =  R2  \n  RankedBaseUrl  =  https://sp.example.com  \n",
        );
        assert_eq!(
            load(&spaced).expect("load"),
            Some(StoredDevice {
                base: "https://sp.example.com".to_string(),
                refresh: "R2".to_string(),
            })
        );
    }

    #[test]
    fn a_refresh_value_without_a_base_is_reported_without_echoing_it() {
        let scratch = Scratch::new();
        let file = scratch.write(
            "half.ini",
            format!("[Settings]\nRankedRefreshToken={REFRESH}\n").as_bytes(),
        );

        let error = load(&file).expect_err("a bound origin is required");

        assert!(message(&error).contains("RankedBaseUrl"), "{error:#}");
        assert!(!message(&error).contains(REFRESH), "{error:#}");
    }

    #[test]
    fn debug_never_prints_the_refresh_value() {
        let device = StoredDevice {
            base: BASE.to_string(),
            refresh: REFRESH.to_string(),
        };

        let rendered = format!("{device:?}");

        assert!(!rendered.contains(REFRESH), "{rendered}");
        assert!(rendered.contains(BASE), "{rendered}");
    }
}
