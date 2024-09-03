import { baseHeaders } from "@/utils/request";
import { API_BASE } from "@/constants";

const Documents = {
  upload: async function (file, workspaceId, sourceUrl = null) {
    const formData = new FormData();
    formData.append("file", file);
    formData.append("workspaceId", workspaceId);
    if (sourceUrl) {
      formData.append("sourceUrl", sourceUrl);
    }

    return await fetch(`${API_BASE}/documents/upload`, {
      method: "POST",
      headers: baseHeaders(),
      body: formData,
    })
      .then((res) => {
        if (!res.ok) throw new Error(res.statusText);
        return res.json();
      })
      .then((res) => res);
  },

  delete: async function (documentId) {
    return await fetch(`${API_BASE}/documents/${documentId}`, {
      method: "DELETE",
      headers: baseHeaders(),
    })
      .then((res) => {
        if (!res.ok) throw new Error(res.statusText);
        return res.json();
      })
      .then((res) => res);
  },

  getUploadStatus: async function (documentId) {
    return await fetch(`${API_BASE}/documents/${documentId}/status`, {
      method: "GET",
      headers: baseHeaders(),
    })
      .then((res) => {
        if (!res.ok) throw new Error(res.statusText);
        return res.json();
      })
      .then((res) => res);
  },
};

export default Documents;
