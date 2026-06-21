/* go-pubsub landing — nav state, copy, count-up, scroll reveal */
(function () {
  'use strict';

  /* ---- nav: tighten on scroll ---- */
  var nav = document.getElementById('nav');
  if (nav) {
    var onScroll = function () {
      nav.classList.toggle('scrolled', window.scrollY > 8);
    };
    window.addEventListener('scroll', onScroll, { passive: true });
    onScroll();
  }

  /* ---- copy install command ---- */
  var copyBtn = document.getElementById('copy-btn');
  var cmdEl = document.getElementById('install-cmd');
  if (copyBtn && cmdEl) {
    var cmd = cmdEl.textContent.trim();
    copyBtn.addEventListener('click', function () {
      var done = function () {
        copyBtn.classList.add('copied');
        var label = copyBtn.querySelector('.hero__copy-label');
        var prev = label ? label.textContent : '';
        if (label) label.textContent = 'Copied';
        setTimeout(function () {
          copyBtn.classList.remove('copied');
          if (label) label.textContent = prev;
        }, 1800);
      };
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(cmd).then(done, done);
      } else {
        var ta = document.createElement('textarea');
        ta.value = cmd; ta.style.position = 'fixed'; ta.style.opacity = '0';
        document.body.appendChild(ta); ta.select();
        try { document.execCommand('copy'); } catch (e) {}
        ta.remove(); done();
      }
    });
  }

  /* ---- count-up for stat numbers ---- */
  function fmt(n, el) {
    if (el.dataset.format === 'k') {
      if (n >= 1000) {
        var k = n / 1000;
        return (Math.round(k * 10) / 10).toString().replace(/\.0$/, '') + 'k';
      }
      return String(n);
    }
    return String(n);
  }
  function animateCount(el) {
    var target = parseInt(el.dataset.count, 10) || 0;
    var suffix = el.dataset.suffix || '';
    if (target === 0) { el.textContent = '0' + suffix; return; }
    var dur = 1300;
    var start = performance.now();
    function tick(now) {
      var p = Math.min((now - start) / dur, 1);
      var eased = 1 - Math.pow(1 - p, 3);
      var cur = Math.round(target * eased);
      el.textContent = fmt(cur, el) + suffix;
      if (p < 1) requestAnimationFrame(tick);
      else el.textContent = fmt(target, el) + suffix;
    }
    requestAnimationFrame(tick);
  }

  /* ---- scroll reveal ---- */
  var revealSel = '.section__head, .card, .code-block, .bench__highlight, .bench__table-wrap, .when__col, .std-card';
  var revealEls = Array.prototype.slice.call(document.querySelectorAll(revealSel));
  revealEls.forEach(function (el) { el.classList.add('reveal'); });

  if ('IntersectionObserver' in window) {
    var io = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) {
          e.target.classList.add('in');
          io.unobserve(e.target);
        }
      });
    }, { threshold: 0.12, rootMargin: '0px 0px -6% 0px' });
    revealEls.forEach(function (el) { io.observe(el); });

    /* count-up when the stat strip scrolls into view */
    var statEls = Array.prototype.slice.call(document.querySelectorAll('.stat__num[data-count]'));
    var statObs = new IntersectionObserver(function (entries) {
      entries.forEach(function (e) {
        if (e.isIntersecting) { animateCount(e.target); statObs.unobserve(e.target); }
      });
    }, { threshold: 0.6 });
    statEls.forEach(function (el) { statObs.observe(el); });
  } else {
    /* no IO — just show everything and set final counts */
    revealEls.forEach(function (el) { el.classList.add('in'); });
    document.querySelectorAll('.stat__num[data-count]').forEach(function (el) {
      el.textContent = fmt(parseInt(el.dataset.count, 10) || 0, el) + (el.dataset.suffix || '');
    });
  }
})();
