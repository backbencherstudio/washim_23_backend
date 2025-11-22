-- CreateTable
CREATE TABLE "ImportJob" (
    "id" TEXT NOT NULL,
    "userId" TEXT NOT NULL,
    "fileType" TEXT NOT NULL,
    "status" TEXT NOT NULL DEFAULT 'PENDING',
    "totalRows" INTEGER NOT NULL DEFAULT 0,
    "importedCount" INTEGER NOT NULL DEFAULT 0,
    "failedCount" INTEGER NOT NULL DEFAULT 0,
    "errorMessage" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ImportJob_pkey" PRIMARY KEY ("id")
);
