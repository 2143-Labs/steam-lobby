//! The determinism gauntlet for the pong sim. The whole rollback design rests
//! on `state[n+1] = step(state[n], inputs[n])` being a pure function, so this
//! file locks that property in from every angle: golden hashes (which the JS
//! mirror must reproduce exactly — see `web/test/golden.mjs`), snapshot
//! roundtrip, rollback equivalence, absence of NaN/-0.0, arrival-order
//! convergence, and the FNV-1a known vectors.
//!
//! The input schedule is identical to the JS one (`web/test/inputs.mjs`):
//! `target(side, frame) = (((frame/5) * 7919 + off) % 997) / 997.0`, with
//! off = 0 (Left) / 331 (Right). Pure integer math, exact in both languages.

use lobby_core::pong::{fnv1a64, PongGame, PongSide, DT_SECS};

fn schedule(frame: u32, side: PongSide) -> f64 {
    let off = match side {
        PongSide::Left => 0u32,
        PongSide::Right => 331u32,
    };
    ((((frame / 5) * 7919) + off) % 997) as f64 / 997.0
}

/// Apply one frame of schedule inputs and step.
fn apply_frame(g: &mut PongGame, frame: u32) {
    g.set_target(PongSide::Left, schedule(frame, PongSide::Left));
    g.set_target(PongSide::Right, schedule(frame, PongSide::Right));
    g.step(DT_SECS);
}

/// Run schedule frames `start..start + frames` with the given (left, right)
/// input offsets. Rollback replays must CONTINUE the frame numbering — never
/// restart at 0, or the schedule and the trajectory diverge.
fn run_schedule(g: &mut PongGame, start: u32, frames: u32, offs: (u32, u32)) {
    for frame in start..start + frames {
        g.set_target(PongSide::Left, ((((frame / 5) * 7919) + offs.0) % 997) as f64 / 997.0);
        g.set_target(PongSide::Right, ((((frame / 5) * 7919) + offs.1) % 997) as f64 / 997.0);
        g.step(DT_SECS);
    }
}

#[test]
fn golden_frame_hashes() {
    let mut g = PongGame::new();
    let mut got: Vec<(u32, u64)> = vec![(0, g.checksum())];
    for frame in 0..10_000 {
        apply_frame(&mut g, frame);
        let n = frame + 1;
        if n == 10 || n == 100 || n == 1000 || n == 10_000 {
            got.push((n, g.checksum()));
        }
    }
    // The five literals were generated once (run, paste) and lock the sim
    // forever. `web/test/golden.mjs` must produce the same values; the M2
    // differential test then proves all 10,000 frames match.
    let expected: Vec<(u32, u64)> = vec![
        (0, 0x5806_9d63_5f54_623d),
        (10, 0x0bfe_6a6d_5a40_b008),
        (100, 0x122e_7cff_f71f_81a4),
        (1000, 0x6d8e_63df_1260_a3cb),
        (10_000, 0x97ff_3c13_e660_3f88),
    ];
    assert_eq!(got, expected);
}

#[test]
fn snapshot_roundtrip_identity() {
    let mut g = PongGame::new();
    for frame in 0..1_000 {
        apply_frame(&mut g, frame);
    }
    let s = g.full_state();
    assert_eq!(s.len(), PongGame::STATE_BYTES);
    let mut h = PongGame::new();
    h.restore(&s);
    assert_eq!(g.full_state(), h.full_state(), "state must roundtrip exactly");
    assert_eq!(g.checksum(), h.checksum());
}

#[test]
fn rollback_equivalence() {
    // The defining property: snapshot at k, run to 1000, restore the snapshot,
    // re-run from k — the final state is bit-identical to running straight
    // through.
    const OFFSETS: [(u32, u32); 3] = [(0, 331), (97, 599), (3, 911)];
    const KS: [u32; 5] = [0, 1, 7, 333, 999];
    const TOTAL: u32 = 1_000;
    for offs in OFFSETS {
        let mut straight = PongGame::new();
        run_schedule(&mut straight, 0, TOTAL, offs);
        for k in KS {
            let mut g = PongGame::new();
            run_schedule(&mut g, 0, k, offs);
            let save = g.full_state();
            run_schedule(&mut g, k, TOTAL - k, offs);
            let final_state = (g.full_state(), g.checksum());

            let mut h = PongGame::new();
            h.restore(&save);
            run_schedule(&mut h, k, TOTAL - k, offs);
            assert_eq!(
                (h.full_state(), h.checksum()),
                final_state,
                "rollback at k={k} (offs {offs:?}) must reproduce the straight-line state"
            );
            assert_eq!(h.full_state(), straight.full_state());
        }
    }
}

#[test]
fn no_nan_no_negzero_never() {
    let mut g = PongGame::new();
    for frame in 0..10_000 {
        apply_frame(&mut g, frame);
        let s = g.full_state();
        for chunk in s.chunks_exact(8).take(9) {
            let bits = u64::from_le_bytes(chunk.try_into().unwrap());
            let v = f64::from_bits(bits);
            assert!(!v.is_nan(), "NaN in state at frame {frame}");
            assert!(!v.is_infinite(), "±Inf in state at frame {frame}");
            assert_ne!(bits, 0x8000_0000_0000_0000, "-0.0 in state at frame {frame}");
        }
    }
}

#[test]
fn arrival_schedule_convergence() {
    // The same (frame -> target) function must produce the identical state no
    // matter how the inputs are delivered: swapped per-frame order, or
    // duplicated deliveries — `set_target` only writes a goal, so the applied
    // sequence is what counts, never the delivery pattern.
    let mut reference = PongGame::new();
    for frame in 0..1_000 {
        apply_frame(&mut reference, frame);
    }

    let mut swapped = PongGame::new();
    for frame in 0..1_000 {
        swapped.set_target(PongSide::Right, schedule(frame, PongSide::Right));
        swapped.set_target(PongSide::Left, schedule(frame, PongSide::Left));
        swapped.step(DT_SECS);
    }
    assert_eq!(swapped.full_state(), reference.full_state(), "order within a frame is irrelevant");

    let mut duplicated = PongGame::new();
    for frame in 0..1_000 {
        duplicated.set_target(PongSide::Left, schedule(frame, PongSide::Left));
        duplicated.set_target(PongSide::Left, schedule(frame, PongSide::Left)); // duplicate
        duplicated.set_target(PongSide::Right, schedule(frame, PongSide::Right));
        duplicated.step(DT_SECS);
    }
    assert_eq!(duplicated.full_state(), reference.full_state(), "duplicate delivery is irrelevant");
}

#[test]
fn fnv1a_known_vectors() {
    // Published FNV-1a 64 test vectors — guards the hand-rolled hash against
    // implementation drift (the JS mirror must match these too).
    assert_eq!(fnv1a64(b""), 0xcbf29ce484222325);
    assert_eq!(fnv1a64(b"a"), 0xaf63dc4c8601ec8c);
}
