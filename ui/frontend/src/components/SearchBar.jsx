import React from "react";

export default function SearchBar({ value, onChange }) {
  return (
    <input
      type="text"
      placeholder="Search datasets..."
      value={value}
      onChange={e => onChange(e.target.value)}
    />
  );
}
