import axios from "axios";

const API_BASE = "http://localhost:8000";

export const fetchDatasets = async (params = {}) => {
  const res = await axios.get(`${API_BASE}/datasets`, { params });
  return res.data;
};
