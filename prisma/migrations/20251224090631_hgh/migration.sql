/*
  Warnings:

  - You are about to drop the `ApolloLead` table. If the table is not empty, all the data it contains will be lost.
  - You are about to drop the `ZoominfoLead` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropForeignKey
ALTER TABLE "ApolloLead" DROP CONSTRAINT "ApolloLead_userId_fkey";

-- DropForeignKey
ALTER TABLE "ZoominfoLead" DROP CONSTRAINT "ZoominfoLead_userId_fkey";

-- DropIndex
DROP INDEX "sales_navigator_leads_email_first_email_second_location_job_idx";

-- DropTable
DROP TABLE "ApolloLead";

-- DropTable
DROP TABLE "ZoominfoLead";

-- CreateTable
CREATE TABLE "zoominfo_leads" (
    "id" TEXT NOT NULL,
    "company_id" TEXT,
    "name" TEXT,
    "email" TEXT,
    "email_score" TEXT,
    "phone" TEXT,
    "work_phone" TEXT,
    "lead_location" TEXT,
    "lead_divison" TEXT,
    "lead_titles" TEXT,
    "seniority_level" TEXT,
    "skills" TEXT,
    "past_companies" TEXT,
    "company_name" TEXT,
    "company_size" TEXT,
    "company_phone_numbers" TEXT,
    "company_location_text" TEXT,
    "company_type" TEXT,
    "company_industry" TEXT,
    "company_sector" TEXT,
    "company_facebook_page" TEXT,
    "revenue_range" TEXT,
    "ebitda_range" TEXT,
    "company_linkedin_page" TEXT,
    "decision_making_power" TEXT,
    "company_function" TEXT,
    "company_funding_range" TEXT,
    "latest_funding_stage" TEXT,
    "company_sic_code" TEXT,
    "company_naics_code" TEXT,
    "company_size_key" TEXT,
    "linkedin_url" TEXT,
    "company_founded_at" TEXT,
    "company_website" TEXT,
    "company_products_services" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "deleted_at" TIMESTAMP(3),
    "userId" TEXT,

    CONSTRAINT "zoominfo_leads_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "apollo_leads" (
    "id" TEXT NOT NULL,
    "first_name" TEXT,
    "last_name" TEXT,
    "title" TEXT,
    "company_name" TEXT,
    "company_name_for_emails" TEXT,
    "email" TEXT,
    "email_status" TEXT,
    "primary_email_source" TEXT,
    "primary_email_verification_source" TEXT,
    "email_confidence" TEXT,
    "primary_email_catch_all_status" TEXT,
    "primary_email_last_verified_at" TEXT,
    "seniority" TEXT,
    "departments" TEXT,
    "contact_owner" TEXT,
    "work_direct_phone" TEXT,
    "home_phone" TEXT,
    "mobile_phone" TEXT,
    "corporate_phone" TEXT,
    "other_phone" TEXT,
    "stage" TEXT,
    "lists" TEXT,
    "last_contacted" TEXT,
    "account_owner" TEXT,
    "employees" TEXT,
    "industry" TEXT,
    "keywords" TEXT,
    "person_linkedin_url" TEXT,
    "website" TEXT,
    "company_uinkedin_url" TEXT,
    "facebook_url" TEXT,
    "twitter_url" TEXT,
    "city" TEXT,
    "state" TEXT,
    "country" TEXT,
    "company_address" TEXT,
    "company_city" TEXT,
    "company_state" TEXT,
    "company_country" TEXT,
    "company_phone" TEXT,
    "technologies" TEXT,
    "annual_revenue" TEXT,
    "total_funding" TEXT,
    "latest_funding" TEXT,
    "latest_funding_amount" TEXT,
    "last_raised_at" TEXT,
    "subsidiary_of" TEXT,
    "email_sent" TEXT,
    "email_open" TEXT,
    "email_bounced" TEXT,
    "replied" TEXT,
    "demoed" TEXT,
    "number_of_retail_locations" TEXT,
    "apollo_contact_id" TEXT,
    "apollo_account_id" TEXT,
    "secondary_email" TEXT,
    "secondary_email_source" TEXT,
    "secondary_email_status" TEXT,
    "secondary_email_verification_source" TEXT,
    "tertiary_email" TEXT,
    "tertiary_email_source" TEXT,
    "tertiary_email_status" TEXT,
    "tertiary_email_verification_source" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "deleted_at" TIMESTAMP(3),
    "userId" TEXT,

    CONSTRAINT "apollo_leads_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "zoominfo_leads_email_lead_titles_company_industry_company_w_idx" ON "zoominfo_leads"("email", "lead_titles", "company_industry", "company_website", "revenue_range", "skills");

-- CreateIndex
CREATE INDEX "apollo_leads_title_industry_keywords_technologies_website_c_idx" ON "apollo_leads"("title", "industry", "keywords", "technologies", "website", "company_uinkedin_url", "country", "city", "state", "annual_revenue");

-- CreateIndex
CREATE INDEX "sales_navigator_leads_job_title_company_domain_url_city_ema_idx" ON "sales_navigator_leads"("job_title", "company_domain", "url", "city", "email_first", "email_second", "location");

-- AddForeignKey
ALTER TABLE "zoominfo_leads" ADD CONSTRAINT "zoominfo_leads_userId_fkey" FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "apollo_leads" ADD CONSTRAINT "apollo_leads_userId_fkey" FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE SET NULL ON UPDATE CASCADE;
