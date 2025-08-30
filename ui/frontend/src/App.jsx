import React, { useEffect, useState } from "react";
import { fetchDatasets } from "./api";
import DatasetTable from "./components/DatasetTable";
import SearchBar from "./components/SearchBar";

export default function App() {
  const [datasets, setDatasets] = useState([]);
  const [searchTerm, setSearchTerm] = useState("");

  useEffect(() => {
    fetchDatasets()
      .then(setDatasets)
      .catch(err => console.error(err));
  }, []);

  const filtered = datasets.filter(d =>
    Object.values(d).some(val =>
      val?.toLowerCase().includes(searchTerm.toLowerCase())
    )
  );

  return (
    <div>
      <header style={{ position: "sticky", top: 0, background: "#fff", zIndex: 10 }}>
        <h1>GovHack 2025 Datasets</h1>
        <SearchBar value={searchTerm} onChange={setSearchTerm} />
        <p>Fetched {datasets.length} datasets</p>
      </header>
      <DatasetTable datasets={filtered} />
    </div>
  );
}
