/*
  Warnings:

  - The values [SELL] on the enum `LeadType` will be removed. If these variants are still used in the database, this will fail.

*/
-- AlterEnum
BEGIN;
CREATE TYPE "LeadType_new" AS ENUM ('SALES_NAVIGATOR', 'ZOOMINFO', 'APOLLO');
ALTER TABLE "LeadData" ALTER COLUMN "type" TYPE "LeadType_new" USING ("type"::text::"LeadType_new");
ALTER TYPE "LeadType" RENAME TO "LeadType_old";
ALTER TYPE "LeadType_new" RENAME TO "LeadType";
DROP TYPE "public"."LeadType_old";
COMMIT;
