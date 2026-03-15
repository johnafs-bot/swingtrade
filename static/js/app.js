// SwingB3 — Main JS

// Auto-dismiss alerts after 5s
document.addEventListener('DOMContentLoaded', function () {
  // Flash messages
  document.querySelectorAll('.alert-dismissible').forEach(el => {
    setTimeout(() => {
      const bsAlert = bootstrap.Alert.getOrCreateInstance(el);
      if (bsAlert) bsAlert.close();
    }, 6000);
  });

  // Tooltips
  const ttEls = document.querySelectorAll('[data-bs-toggle="tooltip"]');
  ttEls.forEach(el => new bootstrap.Tooltip(el));

  // Active nav auto-highlight
  const path = window.location.pathname;
  document.querySelectorAll('.nav-link').forEach(link => {
    if (link.getAttribute('href') === path) {
      link.classList.add('active');
    }
  });
});

// Format currency BRL
function fmtBRL(val) {
  return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(val || 0);
}

// Format percentage
function fmtPct(val, decimals = 1) {
  return (val || 0).toFixed(decimals) + '%';
}

// Color helper for PnL
function pnlColor(val) {
  return val >= 0 ? 'text-success' : 'text-danger';
}
