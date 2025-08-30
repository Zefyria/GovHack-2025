import { useEffect, useState } from "react";

function App() {
  const [datasets, setDatasets] = useState([]);

  useEffect(() => {
    fetch("http://localhost:8000/datasets")
      .then((res) => res.json())
      .then((data) => setDatasets(data.datasets))
      .catch((err) => console.error(err));
  }, []);

  return (
    <div style={{ padding: "20px" }}>
      <h1>GovHack 2025 Datasets</h1>
      <p>Fetched {datasets.length} datasets</p>

      <table border="1" cellPadding="5" cellSpacing="0">
        <thead>
          <tr>
            <th>Source</th>
            <th>Title</th>
            <th>Description</th>
            <th>URL</th>
            <th>Format</th>
          </tr>
        </thead>
        <tbody>
          {datasets.map((dataset, index) => (
            <tr key={index}>
              <td>{dataset.source_name}</td>
              <td>{dataset.title}</td>
              <td>{dataset.description}</td>
              <td>
                <a href={dataset.url} target="_blank" rel="noopener noreferrer">
                  Link
                </a>
              </td>
              <td>{dataset.format}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default App;
