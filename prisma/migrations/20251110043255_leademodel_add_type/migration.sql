/*
  Warnings:

  - Added the required column `type` to the `LeadData` table without a default value. This is not possible if the table is not empty.

*/
-- AlterTable
ALTER TABLE "LeadData" ADD COLUMN     "type" "LeadType" NOT NULL;
