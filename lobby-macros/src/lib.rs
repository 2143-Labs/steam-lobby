use proc_macro::TokenStream;

/// Placeholder — derive macro for GameCallbacks will go here.
#[proc_macro_derive(GameCallbacks)]
pub fn derive_game_callbacks(_item: TokenStream) -> TokenStream {
    TokenStream::new()
}
