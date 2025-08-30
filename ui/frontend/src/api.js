export const API_URL = "http://localhost:8000/datasets";

export async function fetchDatasets() {
  const response = await fetch(API_URL);
  if (!response.ok) throw new Error("Failed to fetch datasets");
  return response.json();
}
