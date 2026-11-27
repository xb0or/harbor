/* 落地页：把示例地址里的占位主机替换为当前访问域名（或 IP），避免写死部署域名。 */
(function () {
  "use strict";
  var origin = location.protocol + "//" + location.host;
  // 模板里统一写成本地占位地址，运行时按实际访问来源替换。
  var placeholder = /https?:\/\/(?:127\.0\.0\.1:8080|localhost:8080)/g;

  function rewriteText(node) {
    var text = node.nodeValue;
    if (!text || !placeholder.test(text)) return;
    placeholder.lastIndex = 0;
    node.nodeValue = text.replace(placeholder, origin);
  }

  function rewrite(root) {
    var walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, null);
    var node;
    while ((node = walker.nextNode())) rewriteText(node);
  }

  function fill() {
    rewrite(document.body);
    // 兜底：仍带占位地址的元素直接改文本（例如被截断的链接）
    var nodes = document.querySelectorAll("code, span, a");
    for (var i = 0; i < nodes.length; i++) {
      var el = nodes[i];
      if (el.children.length === 0 && placeholder.test(el.textContent)) {
        placeholder.lastIndex = 0;
        el.textContent = el.textContent.replace(placeholder, origin);
      }
    }
    placeholder.lastIndex = 0;
    document.title = document.title.replace(placeholder, origin);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", fill);
  } else {
    fill();
  }
})();
