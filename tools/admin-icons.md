# Harbor 管理台 · Lucide 图标

`internal/server/ui/admin.html` 内的全部 51 个 `i-*` symbol 来自官方 **lucide-static@1.43.0** 的 `package/icons/*.svg`。不手画、不加载 CDN、不增加运行时依赖或服务端资源路由。SVG 的几何路径与官方描边属性保持原样，仅将 SVG 容器转换为 symbol、删除节点之间的格式空白并使用 Harbor 的稳定 ID。

## 版本与完整性

- 官方项目：<https://lucide.dev>；包元数据仓库：<https://github.com/lucide-icons/lucide>
- npm 包：`lucide-static@1.43.0`
- 本次工具链：Node.js `v24.13.0`、npm `11.6.2`、Python `3.14.3`
- tarball SHA-512 SRI：`sha512-w7vdVFqh4vv7RxWHN4sSeylA0s3GHV5vpyGqZI0q22MINKJyhMNWSMd90RyTUYWBRNYqdZVZzrMmijVxU3Cl9A==`
- 原始包许可证：`tools/LUCIDE-LICENSE.txt`，包含 Lucide ISC 许可和源于 Feather 的图标 MIT 许可。
- 完整许可同时嵌入 sprite 的 HTML 注释，随 Go 二进制内嵌管理页一起分发。注释内仅移除了不符合 HTML 注释语法的纯文本分隔线，许可正文没有删减。

## 重现（在 harbor 根目录，PowerShell）

```powershell
New-Item -ItemType Directory -Force .snow/lucide-static-1.43.0 | Out-Null
npm pack lucide-static@1.43.0 --ignore-scripts --pack-destination .snow/lucide-static-1.43.0
python tools/generate-admin-icons.py
python tools/generate-admin-icons.py --check
```

生成器使用 Python 标准库直接读取 tarball，不解压到源码目录，不执行安装或包生命周期脚本；写入前验证固定 SHA-512 与包名/版本。`--check` 不写文件，比较所有 symbol 的 XML 几何/描边属性与包中的源文件并核验完整许可证。`--archive` 可指定同一版本 tarball 的其它本地路径。

## 稳定别名

既有的 42 个 symbol ID 全部保留。同名图标直接使用官方文件；语义别名：

| Harbor ID | 官方 SVG |
| --- | --- |
| `i-logo` | `layers-3.svg` |
| `i-queue` | `list-ordered.svg` |
| `i-file` | `file-text.svg` |
| `i-key` | `key-round.svg` |
| `i-home` | `house.svg` |
| `i-out` | `log-out.svg` |
| `i-refresh` | `refresh-cw.svg` |
| `i-trash` | `trash-2.svg` |
| `i-branch` | `git-branch.svg` |
| `i-alert-circle` | `circle-alert.svg` |
| `i-alert-triangle` | `triangle-alert.svg` |
| `i-check-circle-2` | `circle-check-big.svg` |

新增 9 个官方同名 ID：`i-copy`、`i-network`、`i-route`、`i-chevron-down`、`i-sliders-horizontal`、`i-arrow-up`、`i-arrow-down`、`i-circle-help`、`i-loader-circle`。

所有展示用 SVG 容器应保持 `aria-hidden="true" focusable="false"`，操作名称由按钮文本或按钮自身的 `aria-label` 提供。新增动态图标时，先在生成器 `ICONS` 中加入官方文件映射再重新生成，不要手改路径。
