import React from "react";

export default function DatasetTable({ datasets }) {
  return (
    <table border="1" cellPadding="5">
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
        {datasets.map((d, i) => (
          <tr key={i}>
            <td>{d.source_name}</td>
            <td>{d.title}</td>
            <td>{d.description}</td>
            <td>
              <a href={d.url} target="_blank" rel="noopener noreferrer">
                Link
              </a>
            </td>
            <td>{d.format_}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
