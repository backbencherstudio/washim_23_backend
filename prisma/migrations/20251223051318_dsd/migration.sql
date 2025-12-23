/*
  Warnings:

  - You are about to drop the `SalesNavigatorLead` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropForeignKey
ALTER TABLE "SalesNavigatorLead" DROP CONSTRAINT "SalesNavigatorLead_userId_fkey";

-- DropTable
DROP TABLE "SalesNavigatorLead";

-- CreateTable
CREATE TABLE "sales_navigator_leads" (
    "id" TEXT NOT NULL,
    "first_name" TEXT,
    "last_name" TEXT,
    "job_title" TEXT,
    "email_first" TEXT,
    "email_second" TEXT,
    "phone" TEXT,
    "company_phone" TEXT,
    "url" TEXT,
    "company_name" TEXT,
    "company_domain" TEXT,
    "company_id" TEXT,
    "location" TEXT,
    "city" TEXT,
    "linkedin_id" TEXT,
    "created_date" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "deleted_at" TIMESTAMP(3),
    "userId" TEXT,

    CONSTRAINT "sales_navigator_leads_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "sales_navigator_leads_email_first_email_second_location_job_idx" ON "sales_navigator_leads"("email_first", "email_second", "location", "job_title", "url");

-- AddForeignKey
ALTER TABLE "sales_navigator_leads" ADD CONSTRAINT "sales_navigator_leads_userId_fkey" FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE SET NULL ON UPDATE CASCADE;
