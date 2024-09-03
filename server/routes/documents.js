const express = require("express");
const router = express.Router();
const { asyncHandler } = require("../middleware/utils");
const { requireAuth } = require("../middleware/auth");
const {
  singleFileUpload,
  getUploadStatus,
  deleteFile,
} = require("../utils/fileUpload");
const { Workspace } = require("../db/models");
const { Document } = require("../db/models");
const { validateUpload } = require("../utils/validation");

router.post(
  "/upload",
  requireAuth,
  singleFileUpload,
  validateUpload,
  asyncHandler(async (req, res) => {
    const { workspaceId, sourceUrl } = req.body;
    const { filename, originalname, mimetype, size } = req.file;

    const workspace = await Workspace.findByPk(workspaceId);
    if (!workspace) {
      return res.status(404).json({ message: "Workspace not found" });
    }

    const newDocument = await Document.create({
      workspaceId,
      userId: req.user.id,
      filename,
      originalname,
      mimetype,
      size,
      sourceUrl,
    });

    res.status(201).json({
      document: newDocument,
    });
  })
);

router.get(
  "/:documentId/status",
  requireAuth,
  asyncHandler(async (req, res) => {
    const documentId = req.params.documentId;
    const status = await getUploadStatus(documentId);
    res.json({ status });
  })
);

router.delete(
  "/:documentId",
  requireAuth,
  asyncHandler(async (req, res) => {
    const documentId = req.params.documentId;
    const document = await Document.findByPk(documentId);
    if (!document) {
      return res.status(404).json({ message: "Document not found" });
    }
    if (document.userId !== req.user.id) {
      return res.status(403).json({ message: "Forbidden" });
    }
    await deleteFile(document.filename);
    await document.destroy();
    res.json({ message: "Document deleted successfully" });
  })
);

module.exports = router;
