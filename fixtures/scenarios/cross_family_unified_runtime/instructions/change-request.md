# Cross-Family Update Request

请按下面的要求统一处理本次任务：

1. 读取同目录工作区里的 Excel 模板，并把需要填写的空白绩效内容补齐。
2. 把 `settings.yaml` 的 logging 配置统一成 production 模式：
   - `level: info`
   - `pretty: false`
   - `file_output: /var/log/app.log`
3. 最后返回一段执行摘要，明确说明：
   - 更新了哪份 Excel
   - 更新了哪份 structured 配置
   - 主要修改了什么
