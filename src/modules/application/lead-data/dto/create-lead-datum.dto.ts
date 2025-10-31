import { IsOptional, IsString, IsNumber } from 'class-validator';

/**
 * CreateLeadDatumDto
 * Used for type safety and Swagger if you expose individual create/update endpoints.
 * For CSV import, validation is light (optional).
 */
export class CreateLeadDatumDto {
  @IsOptional() @IsString() id?: string;
  @IsOptional() @IsString() type?: string;
  @IsOptional() @IsString() user_id?: string;
  @IsOptional()
  @IsString()
  createdAt?: string;

  // --- sells fields ---
  @IsOptional() @IsString() first_name?: string;
  @IsOptional() @IsString() last_name?: string;
  @IsOptional() @IsString() job_title?: string;
  @IsOptional() @IsString() email_first?: string;
  @IsOptional() @IsString() email_second?: string;
  @IsOptional() @IsString() phone?: string;
  @IsOptional() @IsString() company_phone?: string;
  @IsOptional() @IsString() url?: string;
  @IsOptional() @IsString() company_name?: string;
  @IsOptional() @IsString() company_id?: string;
  @IsOptional() @IsString() company_domain?: string;
  @IsOptional() @IsString() location?: string;
  @IsOptional() @IsString() linkedin_id?: string;
  @IsOptional() @IsString() created_at?: string;

  // --- zoominfo fields ---
  @IsOptional() @IsString() zoominfo_id?: string;
  @IsOptional() @IsString() zoominfo_company_id?: string;
  @IsOptional() @IsString() name?: string;
  @IsOptional() @IsString() email?: string;
  @IsOptional() @IsNumber() email_score?: number;
  @IsOptional() @IsString() work_phone?: string;
  @IsOptional() @IsString() lead_location?: string;
  @IsOptional() @IsString() lead_division?: string;
  @IsOptional() @IsString() lead_title?: string;
  @IsOptional() @IsString() decision_making_power?: string;
  @IsOptional() @IsString() seniority_level?: string;
  @IsOptional() @IsString() linkedin_url?: string;
  @IsOptional() @IsString() company_size?: string;
  @IsOptional() @IsString() company_website?: string;
  @IsOptional() @IsString() company_location_text?: string;
  @IsOptional() @IsString() company_type?: string;
  @IsOptional() @IsString() company_function?: string;
  @IsOptional() @IsString() company_sector?: string;
  @IsOptional() @IsString() company_industry?: string;
  @IsOptional() @IsString() company_founded_at?: string;
  @IsOptional() @IsString() company_funding_range?: string;
  @IsOptional() @IsString() revenue_range?: string;
  @IsOptional() @IsString() ebitda_range?: string;
  @IsOptional() @IsString() last_funding_stage?: string;
  @IsOptional() @IsString() company_size_key?: string;

  // --- apollo/json fields ---
  @IsOptional() @IsString() title?: string;
  @IsOptional() @IsString() company_name_for_emails?: string;
  @IsOptional() @IsString() email_status?: string;
  @IsOptional() @IsString() primary_email_source?: string;
  @IsOptional() @IsString() primary_email_verification_source?: string;
  @IsOptional() @IsString() email_confidence?: string;
  @IsOptional() @IsString() primary_email_catchall_status?: string;
  @IsOptional() @IsString() primary_email_last_verified_at?: string;
  @IsOptional() @IsString() seniority?: string;
  @IsOptional() @IsString() departments?: string;
  @IsOptional() @IsString() contact_owner?: string;
  @IsOptional() @IsString() work_direct_phone?: string;
  @IsOptional() @IsString() home_phone?: string;
  @IsOptional() @IsString() mobile_phone?: string;
  @IsOptional() @IsString() corporate_phone?: string;
  @IsOptional() @IsString() other_phone?: string;
  @IsOptional() @IsString() stage?: string;
  @IsOptional() @IsString() lists?: string;
  @IsOptional() @IsString() last_contacted?: string;
  @IsOptional() @IsString() account_owner?: string;
  @IsOptional() @IsString() employees?: string;
  @IsOptional() @IsString() industry?: string;
  @IsOptional() @IsString() keywords?: string;
  @IsOptional() @IsString() person_linkedin_url?: string;
  @IsOptional() @IsString() website?: string;
  @IsOptional() @IsString() company_linkedin_url?: string;
  @IsOptional() @IsString() facebook_url?: string;
  @IsOptional() @IsString() twitter_url?: string;
  @IsOptional() @IsString() city?: string;
  @IsOptional() @IsString() state?: string;
  @IsOptional() @IsString() country?: string;
  @IsOptional() @IsString() company_address?: string;
  @IsOptional() @IsString() company_city?: string;
  @IsOptional() @IsString() company_state?: string;
  @IsOptional() @IsString() company_country?: string;
  @IsOptional() @IsString() technologies?: string;
  @IsOptional() @IsString() annual_revenue?: string;
  @IsOptional() @IsString() total_funding?: string;
  @IsOptional() @IsString() latest_funding?: string;
  @IsOptional() @IsString() latest_funding_amount?: string;
  @IsOptional() @IsString() last_raised_at?: string;
  @IsOptional() @IsString() subsidiary_of?: string;
  @IsOptional() @IsString() email_sent?: string;
  @IsOptional() @IsString() email_open?: string;
  @IsOptional() @IsString() email_bounced?: string;
  @IsOptional() @IsString() replied?: string;
  @IsOptional() @IsString() demoed?: string;
  @IsOptional() @IsString() number_of_retail_locations?: string;
  @IsOptional() @IsString() apollo_contact_id?: string;
  @IsOptional() @IsString() apollo_account_id?: string;
  @IsOptional() @IsString() secondary_email?: string;
  @IsOptional() @IsString() secondary_email_source?: string;
  @IsOptional() @IsString() secondary_email_status?: string;
  @IsOptional() @IsString() secondary_email_verification_source?: string;
  @IsOptional() @IsString() tertiary_email?: string;
  @IsOptional() @IsString() tertiary_email_source?: string;
  @IsOptional() @IsString() tertiary_email_status?: string;
  @IsOptional() @IsString() tertiary_email_verification_source?: string;
}
