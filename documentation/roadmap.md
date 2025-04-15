---
layout: default
title: Feature Roadmap
nav_order: 2
---

<h2>Feature Roadmap</h2>

<table id="roadmapTable">
  <thead>
    <!-- Language "Tabs" row -->
    <tr>
      <th class="language-tab active" data-language="all">All</th>
      <th class="language-tab" data-language="java">Java</th>
      <th class="language-tab" data-language="go">Go</th>
      <th class="language-tab" data-language="python">Python</th>
      <th colspan="5"></th>
    </tr>
    <!-- Column headers -->
    <tr>
      <th>Service</th>
      <th>..Q4-2024</th>
      <th>Q1-2025</th>
      <th>Q2-2025</th>
      <th>Q3-2025</th>
      <th>Q4-2025</th>
    </tr>
  </thead>
  <tbody>
    <!-- Java rows -->
    <tr data-language="java">
      <td>CredentialsProvider</td>
      <td>AWS,ALI</td>
      <td>-</td>
      <td>GCP</td>
      <td>-</td>
      <td>-</td>
    </tr>
    <tr data-language="java">
      <td>BlobStore</td>
      <td>-</td>
      <td>AWS,ALI</td>
      <td>GCP</td>
      <td>-</td>
      <td>-</td>
    </tr>
    <tr data-language="java">
      <td>Docstore</td>
      <td>-</td>
      <td>AWS,ALI</td>
      <td>-</td>
      <td>GCP</td>
      <td>-</td>
    </tr>
    <tr data-language="java">
      <td>Workflows</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>AWS,GCP,ALI</td>
    </tr>
    <tr data-language="java">
      <td>STS</td>
      <td>AWS,ALI</td>
      <td>-</td>
      <td>GCP</td>
      <td>-</td>
      <td>-</td>
    </tr>
    <tr data-language="java">
      <td>PubSub</td>
      <td>-</td>
      <td>-</td>
      <td>AWS</td>
      <td>ALI,GCP</td>
      <td>-</td>
    </tr>
    <!-- Go rows -->
    <tr data-language="go">
      <td>CredentialsProvider</td>
      <td>-</td>
      <td>-</td>
      <td>AWS,GCP,ALI</td>
      <td>-</td>
      <td>-</td>
    </tr>
    <tr data-language="go">
      <td>BlobStore</td>
      <td>AWS,GCP,ALI</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
    </tr>
    <tr data-language="go">
      <td>Docstore</td>
      <td>AWS,GCP,ALI(v1)</td>
      <td>-</td>
      <td>-</td>
      <td>ALI(v2)</td>
      <td>-</td>
    </tr>
    <tr data-language="go">
      <td>Workflows</td>
      <td>ALI,AWS</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>GCP</td>
    </tr>
    <tr data-language="go">
      <td>STS</td>
      <td>ALI,AWS</td>
      <td>GCP</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
    </tr>
    <tr data-language="go">
      <td>PubSub</td>
      <td>GCP,ALI,AWS</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
    </tr>
    <!-- Python rows -->
    <tr data-language="python">
      <td>CredentialsProvider</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>AWS,GCP</td>
      <td>ALI</td>
    </tr>
    <tr data-language="python">
      <td>BlobStore</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>AWS,GCP</td>
      <td>ALI</td>
    </tr>
    <tr data-language="python">
      <td>Docstore</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>AWS,GCP</td>
      <td>ALI</td>
    </tr>
    <tr data-language="python">
      <td>Workflows</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>AWS,GCP</td>
      <td>ALI</td>
    </tr>
    <tr data-language="python">
      <td>STS</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>AWS,GCP</td>
      <td>ALI</td>
    </tr>
    <tr data-language="python">
      <td>PubSub</td>
      <td>-</td>
      <td>-</td>
      <td>-</td>
      <td>AWS,GCP</td>
      <td>ALI</td>
    </tr>
  </tbody>
</table>

<script>
  const tabs = document.querySelectorAll(".language-tab");
  const rows = document.querySelectorAll("#roadmapTable tbody tr");
  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      tabs.forEach(t => t.classList.remove("active"));
      tab.classList.add("active");
      const selected = tab.getAttribute("data-language");
      rows.forEach(row => {
        row.style.display = selected === "all" || row.getAttribute("data-language") === selected
          ? ""
          : "none";
      });
    });
  });
</script>

<style>
  table {
    border-collapse: collapse;
    width: 100%;
    margin-top: 1em;
    table-layout: auto; /* Let column widths grow based on content */
  }

  th, td {
    border: 1px solid #ccc;
    padding: 0.75em 1em;
    text-align: left;
    vertical-align: top;
    white-space: nowrap;     /* 💡 This prevents splitting/wrapping */
  }

  /* Language tab row styles */
  th.language-tab {
    background-color: #f3f4f6;
    cursor: pointer;
    font-weight: bold;
    transition: background 0.2s ease;
    text-align: center;
  }

  th.language-tab:hover {
    background-color: #e0e7ff;
  }

  th.language-tab.active[data-language="java"] {
    background-color: #f59e0b;
    color: white;
  }

  th.language-tab.active[data-language="go"] {
    background-color: #10b981;
    color: white;
  }

  th.language-tab.active[data-language="python"] {
    background-color: #3b82f6;
    color: white;
  }

  th.language-tab.active[data-language="all"] {
    background-color: #6b7280;
    color: white;
  }
</style>