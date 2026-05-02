document$.subscribe(({body}) => {
  if (typeof mermaid !== 'undefined') {
    const dark = document.body.getAttribute('data-md-color-scheme') === 'slate';
    mermaid.initialize({
      startOnLoad: true,
      theme: dark ? 'dark' : 'default',
      securityLevel: 'loose',
      flowchart: { htmlLabels: true, curve: 'basis' }
    });
    mermaid.run({ querySelector: '.mermaid' });
  }
});
