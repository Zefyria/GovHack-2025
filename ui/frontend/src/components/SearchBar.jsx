import { useState } from "react";

export default function SearchBar({ onSearch }) {
  const [search, setSearch] = useState("");
  const [source, setSource] = useState("");
  const [topic, setTopic] = useState("");

  const handleSubmit = (e) => {
    e.preventDefault();
    onSearch({ search, source, topic });
  };

  return (
    <form onSubmit={handleSubmit} className="mb-4">
      <input
        type="text"
        placeholder="Search..."
        value={search}
        onChange={(e) => setSearch(e.target.value)}
      />
      <select value={source} onChange={(e) => setSource(e.target.value)}>
        <option value="">All Sources</option>
        <option value="ABS">ABS</option>
        <option value="Data.gov.au">Data.gov.au</option>
        <option value="ATO">ATO</option>
      </select>
      <select value={topic} onChange={(e) => setTopic(e.target.value)}>
        <option value="">All Topics</option>
        <option value="Health">Health</option>
        <option value="Finance">Finance</option>
        <option value="Education">Education</option>
        <option value="Population">Population</option>
        <option value="Environment">Environment</option>
        <option value="Other">Other</option>
      </select>
      <button type="submit">Search</button>
    </form>
  );
}
