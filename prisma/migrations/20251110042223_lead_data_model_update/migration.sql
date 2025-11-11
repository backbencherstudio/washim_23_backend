/*
  Warnings:

  - You are about to drop the column `company_function` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `company_funding_range` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `company_linkedin_url` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `createdAt` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `created_at` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `decision_making_power` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `last_funding_stage` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `lead_division` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `lead_title` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `location` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `primary_email_catchall_status` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `type` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `updated_at` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `user_id` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `zoominfo_company_id` on the `LeadData` table. All the data in the column will be lost.
  - You are about to drop the column `zoominfo_id` on the `LeadData` table. All the data in the column will be lost.

*/
-- DropForeignKey
ALTER TABLE "LeadData" DROP CONSTRAINT "LeadData_user_id_fkey";

-- AlterTable
ALTER TABLE "LeadData" DROP COLUMN "company_function",
DROP COLUMN "company_funding_range",
DROP COLUMN "company_linkedin_url",
DROP COLUMN "createdAt",
DROP COLUMN "created_at",
DROP COLUMN "decision_making_power",
DROP COLUMN "last_funding_stage",
DROP COLUMN "lead_division",
DROP COLUMN "lead_title",
DROP COLUMN "location",
DROP COLUMN "primary_email_catchall_status",
DROP COLUMN "type",
DROP COLUMN "updated_at",
DROP COLUMN "user_id",
DROP COLUMN "zoominfo_company_id",
DROP COLUMN "zoominfo_id",
ADD COLUMN     "company_facebook_page" TEXT,
ADD COLUMN     "company_linkedin_page" TEXT,
ADD COLUMN     "company_naics_code" TEXT,
ADD COLUMN     "company_phone_numbers" TEXT,
ADD COLUMN     "company_products_services" TEXT,
ADD COLUMN     "company_sic_code" TEXT,
ADD COLUMN     "company_uinkedin_url" TEXT,
ADD COLUMN     "created_date" TEXT,
ADD COLUMN     "lead_divison" TEXT,
ADD COLUMN     "lead_titles" TEXT,
ADD COLUMN     "past_companies" TEXT,
ADD COLUMN     "primary_email_catch_all_status" TEXT,
ADD COLUMN     "skills" TEXT,
ADD COLUMN     "userId" TEXT,
ALTER COLUMN "email_score" SET DATA TYPE TEXT;

-- AddForeignKey
ALTER TABLE "LeadData" ADD CONSTRAINT "LeadData_userId_fkey" FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE SET NULL ON UPDATE CASCADE;
