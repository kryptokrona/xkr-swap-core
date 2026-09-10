use anyhow::Result;
use vergen_git2::{Emitter, Git2Builder};

fn main() -> Result<()> {
    // Embed a Windows application manifest declaring Common-Controls v6. Some
    // transitive deps (via windows-sys) import `TaskDialogIndirect` from
    // comctl32, which only exists in v6; without a manifest Windows loads v5 and
    // the .exe fails at load with STATUS_ENTRYPOINT_NOT_FOUND (0xC0000139). Gate
    // on the TARGET (CARGO_CFG_WINDOWS) so it works when cross-compiling.
    if std::env::var_os("CARGO_CFG_WINDOWS").is_some() {
        embed_manifest::embed_manifest(embed_manifest::new_manifest("Kryptokrona.Swap"))
            .expect("failed to embed Windows application manifest");
    }

    let git2 = Git2Builder::all_git()?;
    Emitter::default().add_instructions(&git2)?.emit()?;
    Ok(())
}
