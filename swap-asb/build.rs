use anyhow::Result;
use vergen::EmitBuilder;

fn main() -> Result<()> {
    // Embed a Windows manifest declaring Common-Controls v6 so the .exe can load
    // comctl32 v6 (deps import TaskDialogIndirect) instead of failing at load with
    // STATUS_ENTRYPOINT_NOT_FOUND. Gate on the TARGET for cross-compilation.
    if std::env::var_os("CARGO_CFG_WINDOWS").is_some() {
        embed_manifest::embed_manifest(embed_manifest::new_manifest("Kryptokrona.Asb"))
            .expect("failed to embed Windows application manifest");
    }

    EmitBuilder::builder()
        .git_describe(true, true, None)
        .emit()?;
    Ok(())
}
