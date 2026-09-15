use anyhow::Result;
use vergen::EmitBuilder;

fn main() -> Result<()> {
    if std::env::var_os("CARGO_CFG_WINDOWS").is_some() {
        embed_manifest::embed_manifest(embed_manifest::new_manifest("Kryptokrona.Asb"))
            .expect("failed to embed Windows application manifest");
    }

    EmitBuilder::builder()
        .git_describe(true, true, None)
        .emit()?;
    Ok(())
}
