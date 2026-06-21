# PyInstaller spec for agent-gateway
# Usage: pyinstaller agent_gateway.spec

from PyInstaller.utils.hooks import collect_submodules

hidden_imports = collect_submodules('agent_gateway')

# Third-party platform / agent SDKs are imported lazily *inside* adapter and
# bridge methods (e.g. ``from lark_oapi.api.cardkit.v1 import ...`` in the
# Feishu adapter). PyInstaller bundles whatever is importable in the build
# environment, but its static analysis can miss the deep submodules of these
# generated SDKs — the frozen binary would then report
# "Dependencies not installed: pip install agent-gateway[feishu]" at runtime
# for a platform that is actually supported. The CI build installs ``[all]``
# (every platform extra); here we force-include every submodule of each bundled
# SDK so the adapters resolve them at runtime. ``collect_submodules`` on a
# package that isn't installed returns ``[]`` (no error), so this stays safe on
# dev machines that only have ``[desktop]``.
for _sdk in (
    'lark_oapi',        # feishu / lark  — 40+ lazy lark_oapi.api.* services
    'telegram',         # telegram
    'discord',          # discord
    'slack_bolt',       # slack
    'slack_sdk',        # slack
    'claude_code_sdk',  # claude-code-sdk agent bridge
):
    hidden_imports += collect_submodules(_sdk)

a = Analysis(
    ['src/agent_gateway/__main__.py'],
    pathex=['src'],
    binaries=[],
    datas=[],
    hiddenimports=hidden_imports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=None,
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=None)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='agent-gateway',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)