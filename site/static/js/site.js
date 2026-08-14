// Progressive enhancement only: every page is fully readable and navigable
// with this file blocked. Nothing here is required to render content.
(function () {
  "use strict";

  var root = document.documentElement;

  // -------------------------------------------------------------------------
  // Theme
  // -------------------------------------------------------------------------

  function applyTheme(dark) {
    root.dataset.theme = dark ? "dark" : "light";
    var l = document.getElementById("syntax-light");
    var d = document.getElementById("syntax-dark");
    if (l && d) {
      l.media = dark ? "not all" : "all";
      d.media = dark ? "all" : "not all";
    }
    try { localStorage.setItem("theme", dark ? "dark" : "light"); } catch (e) {}
  }

  var toggle = document.getElementById("theme-toggle");
  if (toggle) {
    toggle.addEventListener("click", function () {
      applyTheme(root.dataset.theme !== "dark");
    });
  }

  // Follow the OS until the reader states a preference of their own.
  var mq = window.matchMedia("(prefers-color-scheme: dark)");
  mq.addEventListener("change", function (e) {
    var stored = null;
    try { stored = localStorage.getItem("theme"); } catch (err) {}
    if (!stored) { applyTheme(e.matches); }
  });

  // -------------------------------------------------------------------------
  // Mobile navigation
  // -------------------------------------------------------------------------

  var menu = document.getElementById("menu-toggle");
  var sidebar = document.getElementById("sidebar");
  if (menu && sidebar) {
    menu.addEventListener("click", function () {
      var open = sidebar.classList.toggle("open");
      menu.setAttribute("aria-expanded", String(open));
    });
  }

  // -------------------------------------------------------------------------
  // Table-of-contents scroll spy
  // -------------------------------------------------------------------------

  var tocLinks = Array.prototype.slice.call(
    document.querySelectorAll(".toc a[href^='#']")
  );
  if (tocLinks.length && "IntersectionObserver" in window) {
    var byId = {};
    tocLinks.forEach(function (a) { byId[decodeURIComponent(a.hash.slice(1))] = a; });

    var seen = new Set();
    var observer = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (entry.isIntersecting) { seen.add(entry.target.id); }
        else { seen.delete(entry.target.id); }
      });
      // Highlight the topmost heading currently on screen.
      var first = null;
      Object.keys(byId).forEach(function (id) {
        if (first === null && seen.has(id)) { first = id; }
      });
      tocLinks.forEach(function (a) { a.classList.remove("active"); });
      if (first && byId[first]) { byId[first].classList.add("active"); }
    }, { rootMargin: "-80px 0px -70% 0px" });

    Object.keys(byId).forEach(function (id) {
      var el = document.getElementById(id);
      if (el) { observer.observe(el); }
    });
  }

  // -------------------------------------------------------------------------
  // Language tabs
  // -------------------------------------------------------------------------

  document.querySelectorAll("[data-tabs]").forEach(function (group) {
    var tabs = Array.prototype.slice.call(group.querySelectorAll("[role=tab]"));

    function select(index) {
      tabs.forEach(function (tab, i) {
        var on = i === index;
        tab.setAttribute("aria-selected", String(on));
        tab.tabIndex = on ? 0 : -1;
        var panel = document.getElementById(tab.getAttribute("aria-controls"));
        if (panel) { panel.hidden = !on; }
      });
    }

    tabs.forEach(function (tab, i) {
      tab.addEventListener("click", function () { select(i); });
      tab.addEventListener("keydown", function (e) {
        var next = e.key === "ArrowRight" ? i + 1
                 : e.key === "ArrowLeft" ? i - 1 : null;
        if (next === null) { return; }
        e.preventDefault();
        next = (next + tabs.length) % tabs.length;
        select(next);
        tabs[next].focus();
      });
    });
    select(0);
  });

  // -------------------------------------------------------------------------
  // Search
  //
  // The index is fetched on first use, not on page load: it is the largest
  // asset on the site and most visits never open search.
  // -------------------------------------------------------------------------

  var dialog = document.getElementById("search-dialog");
  var openBtn = document.getElementById("search-open");
  var input = document.getElementById("search-input");
  var results = document.getElementById("search-results");
  if (!dialog || !openBtn || !input || !results || !dialog.showModal) { return; }

  var index = null;
  var loading = null;
  var baseUrl = openBtn.getAttribute("data-base") || "";

  function loadIndex() {
    if (index) { return Promise.resolve(index); }
    if (loading) { return loading; }
    loading = fetch(baseUrl + "search_index.en.json")
      .then(function (r) { return r.json(); })
      .then(function (raw) {
        // Zola's elasticlunr payload: documents keyed by permalink.
        index = Object.keys(raw.documentStore.docs).map(function (id) {
          var doc = raw.documentStore.docs[id];
          return { id: doc.id || id, title: doc.title || "", body: doc.body || "" };
        });
        return index;
      })
      .catch(function () { index = []; return index; });
    return loading;
  }

  function score(doc, terms) {
    var title = doc.title.toLowerCase();
    var body = doc.body.toLowerCase();
    var total = 0;
    for (var i = 0; i < terms.length; i++) {
      var t = terms[i];
      if (!t) { continue; }
      var inTitle = title.indexOf(t) !== -1;
      var at = body.indexOf(t);
      if (!inTitle && at === -1) { return 0; }
      total += inTitle ? 10 : 1;
    }
    return total;
  }

  function excerpt(body, term) {
    var at = body.toLowerCase().indexOf(term);
    if (at === -1) { return body.slice(0, 120); }
    return (at > 30 ? "…" : "") + body.slice(Math.max(0, at - 30), at + 90);
  }

  function render(matches, terms) {
    results.innerHTML = "";
    if (!matches.length) {
      var p = document.createElement("p");
      p.className = "empty";
      p.textContent = "No matches.";
      results.appendChild(p);
      return;
    }
    matches.slice(0, 8).forEach(function (m) {
      var a = document.createElement("a");
      a.href = m.doc.id;
      var strong = document.createElement("strong");
      strong.textContent = m.doc.title;
      var span = document.createElement("span");
      span.textContent = excerpt(m.doc.body, terms[0]);
      a.appendChild(strong);
      a.appendChild(span);
      results.appendChild(a);
    });
  }

  function search() {
    var q = input.value.trim().toLowerCase();
    if (q.length < 2) { results.innerHTML = ""; return; }
    loadIndex().then(function (docs) {
      var terms = q.split(/\s+/);
      var matches = [];
      docs.forEach(function (doc) {
        var s = score(doc, terms);
        if (s > 0) { matches.push({ doc: doc, s: s }); }
      });
      matches.sort(function (a, b) { return b.s - a.s; });
      render(matches, terms);
    });
  }

  input.addEventListener("input", search);

  function open() {
    dialog.showModal();
    input.value = "";
    results.innerHTML = "";
    input.focus();
    loadIndex();
  }

  openBtn.addEventListener("click", open);

  document.addEventListener("keydown", function (e) {
    var typing = /^(INPUT|TEXTAREA|SELECT)$/.test(e.target.tagName) ||
                 e.target.isContentEditable;
    if (!dialog.open && !typing && (e.key === "/" || (e.key === "k" && (e.metaKey || e.ctrlKey)))) {
      e.preventDefault();
      open();
    }
  });

  // Arrow keys move through results; Enter follows the highlighted one.
  input.addEventListener("keydown", function (e) {
    var links = Array.prototype.slice.call(results.querySelectorAll("a"));
    if (!links.length) { return; }
    var at = links.findIndex(function (a) { return a.classList.contains("active"); });

    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      if (at >= 0) { links[at].classList.remove("active"); }
      var next = e.key === "ArrowDown" ? at + 1 : at - 1;
      next = (next + links.length) % links.length;
      links[next].classList.add("active");
      links[next].scrollIntoView({ block: "nearest" });
    } else if (e.key === "Enter" && at >= 0) {
      e.preventDefault();
      window.location.href = links[at].href;
    }
  });
})();
