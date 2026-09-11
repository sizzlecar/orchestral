const tabs = [...document.querySelectorAll('[role="tab"]')];
function selectTab(tab) {
  for (const candidate of tabs) {
    const selected = candidate === tab;
    candidate.setAttribute('aria-selected', String(selected));
    candidate.tabIndex = selected ? 0 : -1;
    document.getElementById(candidate.getAttribute('aria-controls')).hidden = !selected;
  }
}
for (const tab of tabs) {
  tab.addEventListener('click', () => selectTab(tab));
  tab.addEventListener('keydown', event => {
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
    event.preventDefault();
    const index = event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 : (tabs.indexOf(tab) + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
    selectTab(tabs[index]); tabs[index].focus();
  });
}
if (navigator.userAgent.includes('Windows')) selectTab(document.getElementById('tab-windows'));
for (const button of document.querySelectorAll('[data-copy]')) {
  button.addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(document.getElementById(button.dataset.copy).textContent);
      button.textContent = 'Copied';
      document.getElementById('copy-status').textContent = 'Command copied to clipboard.';
      setTimeout(() => { button.textContent = 'Copy'; }, 2000);
    } catch {
      document.getElementById('copy-status').textContent = 'Copy is unavailable. Select and copy the command manually.';
      const range = document.createRange();
      range.selectNodeContents(document.getElementById(button.dataset.copy));
      const selection = window.getSelection();
      selection.removeAllRanges(); selection.addRange(range);
      button.textContent = 'Selected';
    }
  });
}
// The page stays honest before the first release and during API/network failures.
// Enable the published-release state only after every advertised platform has assets.
fetch('https://api.github.com/repos/sizzlecar/orchestral/releases/latest', { signal: AbortSignal.timeout(5000) })
  .then(response => response.ok ? response.json() : null)
  .then(release => {
    if (!release || !/^v\d+\.\d+\.\d+$/.test(release.tag_name) || release.draft || release.prerelease) return;
    const targets = ['aarch64-apple-darwin.tar.gz', 'x86_64-apple-darwin.tar.gz', 'x86_64-unknown-linux-gnu.tar.gz', 'x86_64-pc-windows-msvc.zip'];
    const assets = new Set(release.assets.map(asset => asset.name));
    if (!targets.every(target => ['', '.sha256'].every(suffix => assets.has(`orchestral-${release.tag_name}-${target}${suffix}`)))) return;
    document.getElementById('release-status').textContent = `${release.tag_name} is available for macOS, Linux, and Windows.`;
    document.getElementById('installer-availability').textContent = 'Run the same installer again to upgrade. Your configuration and conversations are preserved.';
    document.getElementById('source-build').open = false;
    document.querySelector('.footnote').textContent = 'Your server and model must support tool calling to perform coding actions.';
  }).catch(() => {});
