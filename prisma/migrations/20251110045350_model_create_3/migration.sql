/*
  Warnings:

  - You are about to drop the `LeadData` table. If the table is not empty, all the data it contains will be lost.

*/
-- DropForeignKey
ALTER TABLE "LeadData" DROP CONSTRAINT "LeadData_userId_fkey";

-- DropTable
DROP TABLE "LeadData";

-- DropEnum
DROP TYPE "LeadType";

-- CreateTable
CREATE TABLE "SalesNavigatorLead" (
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
    "city" TEXT,
    "linkedin_id" TEXT,
    "created_date" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "deleted_at" TIMESTAMP(3),
    "userId" TEXT,

    CONSTRAINT "SalesNavigatorLead_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ZoominfoLead" (
    "id" TEXT NOT NULL,
    "name" TEXT,
    "email" TEXT,
    "email_score" TEXT,
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

    CONSTRAINT "ZoominfoLead_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "ApolloLead" (
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

    CONSTRAINT "ApolloLead_pkey" PRIMARY KEY ("id")
);

-- AddForeignKey
ALTER TABLE "SalesNavigatorLead" ADD CONSTRAINT "SalesNavigatorLead_userId_fkey" FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ZoominfoLead" ADD CONSTRAINT "ZoominfoLead_userId_fkey" FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE SET NULL ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "ApolloLead" ADD CONSTRAINT "ApolloLead_userId_fkey" FOREIGN KEY ("userId") REFERENCES "users"("id") ON DELETE SET NULL ON UPDATE CASCADE;
