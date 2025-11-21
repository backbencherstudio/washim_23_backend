// File: src/config/multer.config.ts

import { diskStorage } from 'multer';

export const multerConfig = {
  // Use diskStorage instead of memory storage
  storage: diskStorage({
    destination: './uploads/temp_csv', // Make sure this directory exists and is writable!
    filename: (req, file, callback) => {
      // Create a unique filename
      const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1e9);
      callback(null, `${uniqueSuffix}-${file.originalname}`);
    },
  }),
  // Keep the file size limit here
  limits: {
    fileSize: 5 * 1024 * 1024 * 1024,
  },
};