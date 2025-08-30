export default function DatasetTable({ datasets }) {
  if (!datasets || datasets.length === 0) return <p>No datasets found.</p>;

  return (
    <table border="1" cellPadding="5">
      <thead>
        <tr>
          <th>Title</th>
          <th>Source</th>
          <th>Topic</th>
          <th>Description</th>
          <th>Format</th>
          <th>Download</th>
        </tr>
      </thead>
      <tbody>
        {datasets.map((d) => (
          <tr key={d.id}>
            <td>{d.title}</td>
            <td>{d.source}</td>
            <td>{d.topic}</td>
            <td>{d.description}</td>
            <td>{d.format}</td>
            <td>
              <a href={d.url} target="_blank" rel="noopener noreferrer">
                {d.format === "xlsx" ? "Download" : "Open"}
              </a>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
