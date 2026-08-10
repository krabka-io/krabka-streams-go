"""Renders the static API reference site from godoc comments.

The hermetic mirror of the Java repository's javadoc site: the Pages workflow
builds //:docsite-site so the published documentation never depends on the
runner's installed Go toolchain.
"""

def _docsite_site_impl(ctx):
    out = ctx.actions.declare_directory(ctx.label.name)
    args = ctx.actions.args()
    args.add("--module", ctx.attr.module)
    args.add("--repo", ctx.attr.repo_url)
    args.add("--out", out.path)
    args.add_all(ctx.files.srcs)
    ctx.actions.run(
        outputs = [out],
        inputs = ctx.files.srcs,
        executable = ctx.executable.tool,
        arguments = [args],
        mnemonic = "GoDocSite",
        progress_message = "Rendering the API reference site",
    )
    return [DefaultInfo(files = depset([out]))]

docsite_site = rule(
    implementation = _docsite_site_impl,
    doc = "Renders the godoc API reference site into a directory.",
    attrs = {
        "srcs": attr.label_list(
            allow_files = [".go"],
            doc = "Go sources; _test.go files contribute runnable examples.",
        ),
        "module": attr.string(
            mandatory = True,
            doc = "The module import path shown on the site.",
        ),
        "repo_url": attr.string(
            doc = "Repository URL for per-symbol source links.",
        ),
        "tool": attr.label(
            default = "//tools/docsite",
            executable = True,
            cfg = "exec",
        ),
    },
)
