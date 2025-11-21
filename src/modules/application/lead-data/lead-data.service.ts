import { BadRequestException, Injectable } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import * as Papa from 'papaparse';
import { PrismaService } from 'src/prisma/prisma.service';

// 💡 FIX: Define a minimal interface to satisfy TypeScript's strict type checking
// when dynamically calling createMany on different model delegates.
interface LeadDelegate {
  createMany: (args: {
    data: any[];
    skipDuplicates: boolean;
  }) => Prisma.PrismaPromise<Prisma.BatchPayload>;
}

@Injectable()
export class LeadDataService {
  private allowedFields = new Set<string>([
    // system + sells + zoominfo + given JSON fields (use exact names you're storing)
    'id',
    'type',
    'user_id',
    'first_name',
    'last_name',
    'job_title',
    'email_first',
    'email_second',
    'phone',
    'company_phone',
    'url',
    'company_name',
    'company_id',
    'company_domain',
    'location',
    'linkedin_id',
    'created_at',
    'zoominfo_id',
    'zoominfo_company_id',
    'name',
    'email',
    'email_score',
    'work_phone',
    'lead_location',
    'lead_division',
    'lead_title',
    'decision_making_power',
    'seniority_level',
    'linkedin_url',
    'company_size',
    'company_website',
    'company_location_text',
    'company_type',
    'company_function',
    'company_sector',
    'company_industry',
    'company_founded_at',
    'company_funding_range',
    'revenue_range',
    'ebitda_range',
    'last_funding_stage',
    'company_size_key',
    // plus the fields from the Apollo JSON - include them all as in your model:
    'title',
    'company_name_for_emails',
    'email_status',
    'primary_email_source',
    'primary_email_verification_source',
    'email_confidence',
    'primary_email_catchall_status',
    'primary_email_last_verified_at',
    'seniority',
    'departments',
    'contact_owner',
    'work_direct_phone',
    'home_phone',
    'mobile_phone',
    'corporate_phone',
    'other_phone',
    'stage',
    'lists',
    'last_contacted',
    'account_owner',
    'employees',
    'industry',
    'keywords',
    'person_linkedin_url',
    'website',
    'company_linkedin_url',
    'facebook_url',
    'twitter_url',
    'city',
    'state',
    'country',
    'company_address',
    'company_city',
    'company_state',
    'company_country',
    'company_phone',
    'technologies',
    'annual_revenue',
    'total_funding',
    'latest_funding',
    'latest_funding_amount',
    'last_raised_at',
    'subsidiary_of',
    'email_sent',
    'email_open',
    'email_bounced',
    'replied',
    'demoed',
    'number_of_retail_locations',
    'apollo_contact_id',
    'apollo_account_id',
    'secondary_email',
    'secondary_email_source',
    'secondary_email_status',
    'secondary_email_verification_source',
    'tertiary_email',
    'tertiary_email_source',
    'tertiary_email_status',
    'tertiary_email_verification_source',
    'createdAt',
    'updated_at',
  ]);

  constructor(private prisma: PrismaService) {}

  async importCsv(csvData: string, type: string, userId: string) {
    if (!userId) throw new BadRequestException('User not found');

    // 1. Define headers based on type
    const expectedHeaders: string[] = [];
    let emailColumns: string[] = [];

    switch (type) {
      case 'SALES_NAVIGATOR':
        expectedHeaders.push(
          'first_name',
          'last_name',
          'job_title',
          'email_first',
          'email_second',
          'phone',
          'company_phone',
          'url',
          'company_name',
          'company_domain',
          'company_id',
          'location',
          'linkedin_id',
          'created_date',
        );
        emailColumns = ['email_first', 'email_second'];
        break;

      case 'ZOOMINFO':
        expectedHeaders.push(
          'company_id',
          'name',
          'email',
          'email_score',
          'phone',
          'work_phone',
          'lead_location',
          'lead_divison',
          'lead_titles',
          'seniority_level',
          'decision_making_power',
          'company_function',
          'company_funding_range',
          'latest_funding_stage',
          'company_name',
          'company_size',
          'company_location_text',
          'company_type',
          'company_industry',
          'company_sector',
          'company_facebook_page',
          'revenue_range',
          'ebitda_range',
          'company_size_key',
          'linkedin_url',
          'company_founded_at',
          'company_website',
          'skills',
          'past_companies',
          'company_phone_numbers',
          'company_linkedin_page',
          'company_sic_code',
          'company_naics_code',
        );
        emailColumns = ['email'];
        break;

      case 'APOLLO':
        expectedHeaders.push(
          'first_name',
          'last_name',
          'title',
          'company_name',
          'company_name_for_emails',
          'email',
          'email_status',
          'primary_email_source',
          'primary_email_verification_source',
          'email_confidence',
          'primary_email_catch_all_status',
          'primary_email_last_verified_at',
          'seniority',
          'departments',
          'contact_owner',
          'work_direct_phone',
          'home_phone',
          'mobile_phone',
          'corporate_phone',
          'other_phone',
          'stage',
          'lists',
          'last_contacted',
          'account_owner',
          'employees',
          'industry',
          'keywords',
          'person_linkedin_url',
          'website',
          'company_uinkedin_url',
          'facebook_url',
          'twitter_url',
          'city',
          'state',
          'country',
          'company_address',
          'company_city',
          'company_state',
          'company_country',
          'company_phone',
          'technologies',
          'annual_revenue',
          'total_funding',
          'latest_funding',
          'latest_funding_amount',
          'last_raised_at',
          'subsidiary_of',
          'email_sent',
          'email_open',
          'email_bounced',
          'replied',
          'demoed',
          'number_of_retail_locations',
          'apollo_contact_id',
          'apollo_account_id',
          'secondary_email',
          'secondary_email_source',
          'secondary_email_status',
          'secondary_email_verification_source',
          'tertiary_email',
          'tertiary_email_source',
          'tertiary_email_status',
          'tertiary_email_verification_source',
        );
        emailColumns = ['email', 'secondary_email', 'tertiary_email'];
        break;

      default:
        throw new BadRequestException('Invalid lead type');
    }

    // 2. Parse CSV
    const parsed = Papa.parse(csvData, {
      header: true,
      skipEmptyLines: 'greedy',
    }); // 'greedy' skips whitespace-only lines

    if (parsed.errors.length && parsed.data.length === 0) {
      // Only fail if NO data could be parsed
      return {
        success: false,
        message: 'CSV Parsing Error',
        errors: parsed.errors,
      };
    }

    // 3. Row Processing
    const cleanedRows = parsed.data
      .map((row: any) => {
        // Initialize object with all expected keys set to NULL
        const cleanRow: any = {};
        expectedHeaders.forEach((h) => (cleanRow[h] = null));

        // Fill with data from CSV
        Object.keys(row).forEach((key) => {
          // Only process if the key matches our expected headers (security & cleanup)
          if (expectedHeaders.includes(key)) {
            let value = String(row[key] || '').trim();

            // Cleanup junk values
            if (value.toLowerCase() === 'nan' || value === '') {
              value = null;
            } else {
              // Remove extra quotes
              value = value.replace(/^(\[?['"]\]?)+|(['"]\]?)+$/g, '');
              // Truncate if too long
              if (value.length > 1000) value = value.substring(0, 1000);
            }

            // Relaxed Email Logic: If invalid, set to NULL (don't reject row)
            if (emailColumns.includes(key) && value) {
              if (!value.includes('@')) {
                value = null; // Invalid email becomes null
              }
            }

            cleanRow[key] = value;
          }
        });

        // Check if the row is completely empty (all values null)
        const hasData = Object.values(cleanRow).some((v) => v !== null);
        if (!hasData) return null;

        // Attach User ID
        cleanRow['userId'] = userId;
        return cleanRow;
      })
      .filter((r) => r !== null); // Remove completely empty rows

    // 4. Insert Valid Rows (Batch Insertion)
    if (cleanedRows.length) {
      const BATCH_SIZE = 5000; // Optimal batch size to avoid V8 string length limit
      let insertedCount = 0;

      // Get the raw delegate union type
      const rawDelegate =
        type === 'SALES_NAVIGATOR'
          ? this.prisma.salesNavigatorLead
          : type === 'ZOOMINFO'
            ? this.prisma.zoominfoLead
            : type === 'APOLLO'
              ? this.prisma.apolloLead
              : null;

      if (!rawDelegate) {
        throw new BadRequestException('Invalid lead type for batch insertion');
      }

      // Assert the delegate type to ensure createMany is callable
      const delegate = rawDelegate as unknown as LeadDelegate;

      try {
        // Iterate through data in chunks (batches)
        for (let i = 0; i < cleanedRows.length; i += BATCH_SIZE) {
          const batch = cleanedRows.slice(i, i + BATCH_SIZE);

          // Use createMany for the small batch
          const result = await delegate.createMany({
            data: batch,
            skipDuplicates: true,
          });
          insertedCount += result.count;
        }

        return {
          success: true,
          imported: insertedCount, // Total count from all successful batches
          type,
          message: `${insertedCount} ${type} leads imported successfully.`,
          errors: null,
        };
      } catch (error) {
        console.error('Database Insert Error:', error);
        throw new BadRequestException(
          'Failed to save data to database. Check fields or reduce batch size.',
        );
      }
    }

    return {
      success: true,
      imported: 0,
      type,
      message: `No valid ${type} leads found to import.`,
      errors: null,
    };
  }

  // ========================================================
  // 🔹 COMMON PAGINATION + FILTER LOGIC (DYNAMIC FIND ALL)
  // ========================================================
  private async dynamicFindAll(
    model: 'salesNavigatorLead' | 'zoominfoLead' | 'apolloLead' | 'leadData',
    query: Record<string, any>,
    user: any,
  ) {
    // Pagination setup
    const page = Number(query.page) > 0 ? Number(query.page) : 1;
    const limit = Number(query.limit) > 0 ? Number(query.limit) : 20;
    const skip = (page - 1) * limit;

    const where: any = {};

    // Multi-name search
    if (query.name) {
      const names = query.name.split(/[,\s]+/).filter(Boolean);
      where.OR = names.map((n: string) => ({
        name: { contains: n, mode: 'insensitive' },
      }));
    }

    // Generic multi-field filters
    for (const key of Object.keys(query)) {
      if (['page', 'limit', 'name', 'sortBy', 'order'].includes(key)) continue;
      const value = query[key];
      if (!value) continue;

      let values: string[] = [];
      try {
        values = Array.isArray(value) ? value : JSON.parse(value);
        if (!Array.isArray(values)) values = [String(value)];
      } catch {
        values = [String(value)];
      }

      if (!Array.isArray(where.AND)) {
        where.AND = where.AND ? [where.AND as any] : [];
      }
      (where.AND as any[]).push({
        OR: values.map((v) => ({
          [key]: { contains: v, mode: 'insensitive' },
        })),
      });
    }

    // Sorting
    const sortBy = query.sortBy || 'created_at';
    const order =
      query.order && ['asc', 'desc'].includes(query.order.toLowerCase())
        ? (query.order.toLowerCase() as Prisma.SortOrder)
        : Prisma.SortOrder.desc;

    const orderBy = { [sortBy]: order };

    // get the delegate for the requested model and assert it has findMany/count
    const delegate: {
      findMany: (args?: any) => Promise<any[]>;
      count: (args?: any) => Promise<number>;
    } = (this.prisma as any)[model];

    // Guest: only 20 total
    if (!user) {
      const data = await delegate.findMany({
        where,
        take: 20,
        orderBy,
      });

      if (data.length == 0) {
        return {
          success: false,
          message: 'Data not found. Please import data first.',
        };
      }

      return {
        success: true,
        message: 'Get All Data',
        data,
        meta: {
          total: data.length,
          page: 1,
          limit: 20,
          pages: 1,
        },
        access: 'guest',
      };
    }

    // Authorized: full pagination
    const [data, total] = await Promise.all([
      delegate.findMany({
        where,
        take: limit,
        skip,
        orderBy,
      }),
      delegate.count({ where }),
    ]);

    return {
      data,
      meta: {
        total,
        page,
        limit,
        pages: Math.ceil(total / limit),
      },
      access: 'authorized',
    };
  }

  // Build prisma where/orderBy from query
  private buildPrismaQuery(
    query: any,
    pagination?: { take: number; skip: number },
  ) {
    const q = query.q?.trim();
    const where: any = {};
    const orderBy: any = {};

    // Field-specific filters: any query param that matches allowedFields
    for (const key of Object.keys(query)) {
      if (['page', 'limit', 'q', 'sortBy', 'order'].includes(key)) continue;
      if (this.allowedFields.has(key)) {
        // simple contains search for string-like fields, exact for numeric
        const val = query[key];
        if (val === undefined || val === '') continue;
        // try numeric
        if (!Number.isNaN(Number(val))) {
          where[key] = Number(val);
        } else {
          where[key] = { contains: String(val), mode: 'insensitive' };
        }
      }
    }

    // Global q search across multiple text fields
    if (q) {
      // choose a subset of fields to search (text-like). We'll search many fields
      const textFields = Array.from(this.allowedFields).filter((f) =>
        [
          'first_name',
          'last_name',
          'job_title',
          'email_first',
          'email_second',
          'company_name',
          'url',
          'location',
          'name',
          'email',
          'lead_title',
          'linkedin_url',
          'company_website',
          'company_location_text',
          'industry',
          'keywords',
          'person_linkedin_url',
          'website',
          'company_linkedin_url',
          'city',
          'state',
          'country',
          'technologies',
        ].includes(f),
      );

      where.OR = textFields.map((f) => ({
        [f]: { contains: q, mode: 'insensitive' },
      }));
    }

    // order
    if (query.sortBy && this.allowedFields.has(query.sortBy)) {
      const ord =
        query.order && query.order.toLowerCase() === 'asc' ? 'asc' : 'desc';
      orderBy[query.sortBy] = ord;
    } else {
      orderBy['createdAt'] = 'desc';
    }

    return {
      take: pagination?.take,
      skip: pagination?.skip,
      where,
      orderBy: orderBy as any,
    };
  }

  // ====================Apollo=========================================
  // ====================Apollo=========================================
  // ====================Apollo=========================================
  // ==================== APOLLO LEADS ====================

  async findAllApollo(query: Record<string, any>, user: any) {
    return this.ApolloLead('apolloLead', query, user);
  }

  private async ApolloLead(
    model: 'apolloLead',
    query: Record<string, any>,
    user: any,
  ) {
    // Pagination setup
    const page = Number(query.page) > 0 ? Number(query.page) : 1;
    const limit = Number(query.limit) > 0 ? Number(query.limit) : 20;
    const skip = (page - 1) * limit;

    const where: any = {
      deleted_at: null,
      AND: [], // Initialize AND array
    };

    // ------------------------------------------------
    // 1. Annual Revenue Range Logic
    // ------------------------------------------------
    const minRevenueStr = query.min_annual_revenue;
    const maxRevenueStr = query.max_annual_revenue;

    if (minRevenueStr || maxRevenueStr) {
      const minVal = minRevenueStr ? parseFloat(minRevenueStr) : 0;
      const maxVal = maxRevenueStr ? parseFloat(maxRevenueStr) : Infinity;

      if (isNaN(minVal) || isNaN(maxVal) || minVal > maxVal) {
        throw new BadRequestException('Invalid revenue range provided.');
      }

      // Add the filter to the AND array.
      const revenueFilter: any = { gte: minVal.toString() };
      if (maxRevenueStr) {
        revenueFilter.lte = maxVal.toString();
      }

      where.AND.push({
        annual_revenue: revenueFilter,
      });
    }

    // ------------------------------------------------
    // 2. Employee Range Logic
    // ------------------------------------------------
    const minEmployeeStr = query.min_employee; // Use min_employee from query
    const maxEmployeeStr = query.max_employee; // Use max_employee from query

    if (minEmployeeStr || maxEmployeeStr) {
      // Convert user inputs to numbers
      const minVal = minEmployeeStr ? parseFloat(minEmployeeStr) : 0;
      const maxVal = maxEmployeeStr ? parseFloat(maxEmployeeStr) : Infinity;

      // Check for basic validation
      if (isNaN(minVal) || isNaN(maxVal) || minVal > maxVal) {
        throw new BadRequestException('Invalid employee count range provided.');
      }

      // This is the CORRECT filter object structure for the 'employees' column (Note: uses string comparison)
      const employeeFilter: any = { gte: minVal.toString() };
      if (maxEmployeeStr) {
        employeeFilter.lte = maxVal.toString();
      }

      // Push the filter that targets the correct database column 'employees'
      where.AND.push({
        employees: employeeFilter,
      });
    }

    // ------------------------------------------------
    // 3. Global Search (q) Logic
    // ------------------------------------------------
    if (query.q) {
      const searchTerm = String(query.q).trim();

      where.AND.push({
        OR: [
          { first_name: { contains: searchTerm, mode: 'insensitive' } },
          { last_name: { contains: searchTerm, mode: 'insensitive' } },
          { title: { contains: searchTerm, mode: 'insensitive' } },
          { company_name: { contains: searchTerm, mode: 'insensitive' } },
          { email: { contains: searchTerm, mode: 'insensitive' } },
          { city: { contains: searchTerm, mode: 'insensitive' } },
          { country: { contains: searchTerm, mode: 'insensitive' } },
          { industry: { contains: searchTerm, mode: 'insensitive' } },
          { keywords: { contains: searchTerm, mode: 'insensitive' } },
          { website: { contains: searchTerm, mode: 'insensitive' } },
        ],
      });
    }

    // ------------------------------------------------
    // 4. Specific/Generic Filter Loop
    // ------------------------------------------------
    for (const key of Object.keys(query)) {
      // FIX: Include all min/max query parameters in the skip list
      if (
        [
          'page',
          'limit',
          'sortBy',
          'order',
          'q',
          'min_annual_revenue',
          'max_annual_revenue',
          'min_employee',
          'max_employee',
        ].includes(key)
      )
        continue;

      const value = query[key];
      if (!value) continue;

      let values: string[] = [];
      try {
        values = Array.isArray(value) ? value : JSON.parse(value);
        if (!Array.isArray(values)) values = [String(value)];
      } catch {
        values = [String(value)];
      }

      // Custom filtering blocks (name, job_titles, keyword, etc.)
      if (key === 'name') {
        where.AND.push({
          OR: values.flatMap((v) => [
            { first_name: { contains: v, mode: 'insensitive' } },
            { last_name: { contains: v, mode: 'insensitive' } },
          ]),
        });
      }
      // ... (other custom blocks like job_titles, keyword, company_linkedin, etc. remain the same)
      else if (key === 'job_titles' || key === 'job_titless') {
        where.AND.push({
          OR: values.map((v) => ({
            title: { contains: v, mode: 'insensitive' },
          })),
        });
      } else if (key === 'keyword') {
        where.AND.push({
          OR: values.map((v) => ({
            keywords: { contains: v, mode: 'insensitive' },
          })),
        });
      }
      // ... (other custom blocks)
      else {
        // Default generic filter
        where.AND.push({
          OR: values.map((v) => ({
            [key]: { contains: v, mode: 'insensitive' },
          })),
        });
      }
    }

    if (where.AND.length === 0) {
      delete where.AND;
    }

    // ------------------------------------------------
    // 5. Final Query Execution
    // ------------------------------------------------
    const sortBy = query.sortBy || 'created_at';
    const order =
      query.order && ['asc', 'desc'].includes(query.order.toLowerCase())
        ? (query.order.toLowerCase() as Prisma.SortOrder)
        : Prisma.SortOrder.desc;
    const orderBy = { [sortBy]: order };

    const delegate: {
      findMany: (args?: any) => Promise<any[]>;
      count: (args?: any) => Promise<number>;
    } = (this.prisma as any)[model];

    const [data, total] = await Promise.all([
      delegate.findMany({
        where,
        take: limit,
        skip,
        orderBy,
      }),
      delegate.count({ where }),
    ]);

    return {
      success: true,
      message: 'Data fetched successfully',
      data,
      meta: {
        total,
        page,
        limit,
        pages: Math.ceil(total / limit),
      },
      access: 'authorized',
    };
  }

  async getJobTitles(search?: string) {
    try {
      const titles = await this.prisma.apolloLead.findMany({
        where: {
          title: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, title: true },
        distinct: ['title'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return { success: true, message: 'All job titles fetched', data: titles };
    } catch {
      throw new BadRequestException('Job titles fetch failed');
    }
  }

  async getIndustry(search?: string) {
    try {
      const industries = await this.prisma.apolloLead.findMany({
        where: {
          industry: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, industry: true },
        distinct: ['industry'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'All industries fetched',
        data: industries,
      };
    } catch {
      throw new BadRequestException('Industries fetch failed');
    }
  }

  async getKeyword(search?: string) {
    try {
      const keywordBlobs = await this.prisma.apolloLead.findMany({
        where: {
          keywords: {
            contains: search, // প্রাথমিক ফিল্টার
            mode: 'insensitive',
          },
        },
        select: { keywords: true },
        distinct: ['keywords'],
        take: 100,
      });

      if (!keywordBlobs.length) {
        return { success: true, message: 'No keywords found', data: [] };
      }

      const matchingKeywords = new Set<string>();

      for (const item of keywordBlobs) {
        if (!item.keywords) continue;

        const individualKeywords = item.keywords.split(',');

        for (let keyword of individualKeywords) {
          keyword = keyword
            .trim() // বাইরের স্পেস
            .replace(/^"|"$/g, '')
            .replace(/^'|'$/g, '')
            .trim();

          if (
            search &&
            keyword &&
            keyword.toLowerCase().includes(search.toLowerCase())
          ) {
            matchingKeywords.add(keyword);
          } else if (!search && keyword) {
            matchingKeywords.add(keyword);
          }
        }
      }

      const uniqueKeywordList = Array.from(matchingKeywords);
      const limitedList = uniqueKeywordList.slice(0, 10);

      const finalData = limitedList.map((keyword) => ({
        id: keyword,
        keywords: keyword,
      }));

      return {
        success: true,
        message: 'All keywords fetched',
        data: finalData,
      };
    } catch (e) {
      console.error(e);
      throw new BadRequestException('Keywords fetch failed');
    }
  }

  async getTechnologies(search?: string) {
    try {
      const technologyBlobs = await this.prisma.apolloLead.findMany({
        where: {
          technologies: {
            contains: search,
            mode: 'insensitive',
          },
        },
        select: { technologies: true },
        distinct: ['technologies'],
        take: 100,
      });

      if (!technologyBlobs.length) {
        return { success: true, message: 'No technologies found', data: [] };
      }

      const matchingTechnologies = new Set<string>();

      for (const item of technologyBlobs) {
        if (!item.technologies) continue;

        const individualTechnologies = item.technologies.split(',');

        for (let technology of individualTechnologies) {
          technology = technology
            .trim() // বাইরের স্পেস
            .replace(/^"|"$/g, '')
            .replace(/^'|'$/g, '')
            .trim();

          if (
            search &&
            technology &&
            technology.toLowerCase().includes(search.toLowerCase())
          ) {
            matchingTechnologies.add(technology);
          } else if (!search && technology) {
            matchingTechnologies.add(technology);
          }
        }
      }

      const uniqueTechnologyList = Array.from(matchingTechnologies);
      const limitedList = uniqueTechnologyList.slice(0, 10);

      const finalData = limitedList.map((technology) => ({
        id: technology,
        technologies: technology,
      }));

      return {
        success: true,
        message: 'All technologies fetched',
        data: finalData,
      };
    } catch (e) {
      console.error(e);
      throw new BadRequestException('Technologies fetch failed');
    }
  }

  async getWebsite(search?: string) {
    try {
      const websites = await this.prisma.apolloLead.findMany({
        where: {
          website: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, website: true },
        distinct: ['website'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return { success: true, message: 'All websites fetched', data: websites };
    } catch {
      throw new BadRequestException('Websites fetch failed');
    }
  }

  async getCompanyLinkedin(search?: string) {
    try {
      const linkedinUrls = await this.prisma.apolloLead.findMany({
        where: {
          company_uinkedin_url: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, company_uinkedin_url: true },
        distinct: ['company_uinkedin_url'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'All company LinkedIn URLs fetched',
        data: linkedinUrls,
      };
    } catch {
      throw new BadRequestException('Company LinkedIn fetch failed');
    }
  }

  async getCountry(search?: string) {
    try {
      const countries = await this.prisma.apolloLead.findMany({
        where: {
          country: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, country: true },
        distinct: ['country'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'All countries fetched',
        data: countries,
      };
    } catch {
      throw new BadRequestException('Countries fetch failed');
    }
  }

  async getCity(search?: string) {
    try {
      const cities = await this.prisma.apolloLead.findMany({
        where: {
          city: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, city: true },
        distinct: ['city'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return { success: true, message: 'All cities fetched', data: cities };
    } catch {
      throw new BadRequestException('Cities fetch failed');
    }
  }

  async getState(search?: string) {
    try {
      const states = await this.prisma.apolloLead.findMany({
        where: {
          state: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, state: true },
        distinct: ['state'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return { success: true, message: 'All states fetched', data: states };
    } catch {
      throw new BadRequestException('States fetch failed');
    }
  }

  async getAnnualRevenue(search?: string) {
    try {
      const revenues = await this.prisma.apolloLead.findMany({
        where: {
          annual_revenue: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, annual_revenue: true },
        distinct: ['annual_revenue'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'All annual revenues fetched',
        data: revenues,
      };
    } catch {
      throw new BadRequestException('Annual revenues fetch failed');
    }
  }

  async getDemoed(search?: string) {
    try {
      const demoed = await this.prisma.apolloLead.findMany({
        where: {
          demoed: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, demoed: true },
        distinct: ['demoed'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'All demoed data fetched',
        data: demoed,
      };
    } catch {
      throw new BadRequestException('Demoed fetch failed');
    }
  }

  // ====================ZoomInfo=========================================
  // ====================ZoomInfo=========================================
  // ====================ZoomInfo=========================================

  async findAllZoominfo(query: Record<string, any>, user: any) {
    const zoominfoData = await this.ZoominfoLead('zoominfoLead', query, user);

    return zoominfoData;
  }

  private async ZoominfoLead(
    model: 'zoominfoLead',
    query: Record<string, any>,
    user: any,
  ) {
    const page = Number(query.page) > 0 ? Number(query.page) : 1;
    const limit = Number(query.limit) > 0 ? Number(query.limit) : 20;
    const skip = (page - 1) * limit;

    const where: any = { AND: [] };

    if (query.q) {
      const search = String(query.q).trim();

      where.AND.push({
        OR: [
          { company_id: { contains: search, mode: 'insensitive' } },
          { name: { contains: search, mode: 'insensitive' } },
          { email: { contains: search, mode: 'insensitive' } },
          { email_score: { contains: search, mode: 'insensitive' } },
          { phone: { contains: search, mode: 'insensitive' } },
          { work_phone: { contains: search, mode: 'insensitive' } },
          { lead_location: { contains: search, mode: 'insensitive' } },
          { lead_divison: { contains: search, mode: 'insensitive' } },
          { lead_titles: { contains: search, mode: 'insensitive' } },
          { seniority_level: { contains: search, mode: 'insensitive' } },
          { skills: { contains: search, mode: 'insensitive' } },
          { past_companies: { contains: search, mode: 'insensitive' } },
          { company_name: { contains: search, mode: 'insensitive' } },
          { company_size: { contains: search, mode: 'insensitive' } },
          { company_phone_numbers: { contains: search, mode: 'insensitive' } },
          { company_location_text: { contains: search, mode: 'insensitive' } },
          { company_type: { contains: search, mode: 'insensitive' } },
          { company_industry: { contains: search, mode: 'insensitive' } },
          { company_sector: { contains: search, mode: 'insensitive' } },
          { company_facebook_page: { contains: search, mode: 'insensitive' } },
          { revenue_range: { contains: search, mode: 'insensitive' } },
          { ebitda_range: { contains: search, mode: 'insensitive' } },
          { company_linkedin_page: { contains: search, mode: 'insensitive' } },
          { decision_making_power: { contains: search, mode: 'insensitive' } },
          { company_function: { contains: search, mode: 'insensitive' } },
          { company_funding_range: { contains: search, mode: 'insensitive' } },
          { latest_funding_stage: { contains: search, mode: 'insensitive' } },
          { company_sic_code: { contains: search, mode: 'insensitive' } },
          { company_naics_code: { contains: search, mode: 'insensitive' } },
          { company_size_key: { contains: search, mode: 'insensitive' } },
          { linkedin_url: { contains: search, mode: 'insensitive' } },
          { company_founded_at: { contains: search, mode: 'insensitive' } },
          { company_website: { contains: search, mode: 'insensitive' } },
          {
            company_products_services: {
              contains: search,
              mode: 'insensitive',
            },
          },
        ],
      });
    }

    for (const key of Object.keys(query)) {
      if (
        ['page', 'limit', 'name', 'sortBy', 'order', 'q', 'search'].includes(
          key,
        )
      )
        continue;

      const value = query[key];
      if (!value) continue;

      let values: string[] = [];
      try {
        values = Array.isArray(value) ? value : JSON.parse(value);
        if (!Array.isArray(values)) values = [String(value)];
      } catch {
        values = [String(value)];
      }

      where.AND.push({
        OR: values.map((v) => ({
          [key]: { contains: v, mode: 'insensitive' },
        })),
      });
    }

    if (where.AND.length === 0) {
      delete where.AND;
    }

    // Sorting setup
    const sortBy = query.sortBy || 'created_at';
    const order =
      query.order && ['asc', 'desc'].includes(query.order.toLowerCase())
        ? (query.order.toLowerCase() as Prisma.SortOrder)
        : Prisma.SortOrder.desc;

    const orderBy = { [sortBy]: order };

    const delegate = (this.prisma as any)[model];

    const data = await delegate.findMany({
      where,
      take: limit,
      skip,
      orderBy,
    });

    const total = await delegate.count({ where });

    return {
      success: true,
      message: 'Data fetched successfully',
      data,
      meta: {
        total,
        page,
        limit,
        pages: Math.ceil(total / limit),
      },
      access: 'authorized',
    };
  }
  async getEmail(search?: string) {
    try {
      const data = await this.prisma.zoominfoLead.findMany({
        where: {
          email: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, email: true },
        distinct: ['email'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return { success: true, message: 'Emails fetched successfully', data };
    } catch {
      throw new BadRequestException('Email fetch failed');
    }
  }

  async getLeadTitles(search?: string) {
    try {
      const data = await this.prisma.zoominfoLead.findMany({
        where: {
          lead_titles: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, lead_titles: true },
        distinct: ['lead_titles'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'Lead titles fetched successfully',
        data,
      };
    } catch {
      throw new BadRequestException('Lead titles fetch failed');
    }
  }

  async getCompanyIndustry(search?: string) {
    try {
      const data = await this.prisma.zoominfoLead.findMany({
        where: {
          company_industry: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, company_industry: true },
        distinct: ['company_industry'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'Company industries fetched successfully',
        data,
      };
    } catch {
      throw new BadRequestException('Company industry fetch failed');
    }
  }

  async getCompanyWebsite(search?: string) {
    try {
      const data = await this.prisma.zoominfoLead.findMany({
        where: {
          company_website: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, company_website: true },
        distinct: ['company_website'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'Company websites fetched successfully',
        data,
      };
    } catch {
      throw new BadRequestException('Company website fetch failed');
    }
  }

  async getRevenueRange(search?: string) {
    try {
      const data = await this.prisma.zoominfoLead.findMany({
        where: {
          revenue_range: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, revenue_range: true },
        distinct: ['revenue_range'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'Revenue ranges fetched successfully',
        data,
      };
    } catch {
      throw new BadRequestException('Revenue range fetch failed');
    }
  }

  async getCompanySize(search?: string) {
    try {
      const data = await this.prisma.zoominfoLead.findMany({
        where: {
          company_size: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, company_size: true },
        distinct: ['company_size'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'Company sizes fetched successfully',
        data,
      };
    } catch {
      throw new BadRequestException('Company size fetch failed');
    }
  }

  async getCompanyLocationText(search?: string) {
    try {
      const data = await this.prisma.zoominfoLead.findMany({
        where: {
          company_location_text: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, company_location_text: true },
        distinct: ['company_location_text'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'Company locations fetched successfully',
        data,
      };
    } catch {
      throw new BadRequestException('Company location fetch failed');
    }
  }

  async getCompanySizeKey(search?: string) {
    try {
      const data = await this.prisma.zoominfoLead.findMany({
        where: {
          company_size_key: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, company_size_key: true },
        distinct: ['company_size_key'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'Company size keys fetched successfully',
        data,
      };
    } catch {
      throw new BadRequestException('Company size key fetch failed');
    }
  }

  async getSkill(search?: string) {
    try {
      const skillBlobs = await this.prisma.zoominfoLead.findMany({
        where: {
          skills: {
            contains: search,
            mode: 'insensitive',
          },
        },
        select: { skills: true },
        distinct: ['skills'],
        take: 100,
      });

      if (!skillBlobs.length) {
        return { success: true, message: 'No keywords found', data: [] };
      }

      const matchingSkills = new Set<string>();

      for (const item of skillBlobs) {
        if (!item.skills) continue;

        const individualSkills = item.skills.split(',');

        for (let skill of individualSkills) {
          skill = skill
            .trim() // e.g., " 'Sourcing' " -> "'Sourcing'"
            .replace(/^"|"$/g, '')
            .replace(/^'|'$/g, '')
            .trim();

          if (
            search &&
            skill &&
            skill.toLowerCase().includes(search.toLowerCase())
          ) {
            matchingSkills.add(skill);
          } else if (!search && skill) {
            matchingSkills.add(skill);
          }
        }
      }

      const uniqueSkillList = Array.from(matchingSkills);
      const limitedList = uniqueSkillList.slice(0, 10);

      const finalData = limitedList.map((skill) => ({
        id: skill,
        skills: skill,
      }));

      return {
        success: true,
        message: 'Keywords fetched successfully',
        data: finalData,
      };
    } catch (e) {
      console.error(e);
      throw new BadRequestException('Keyword fetch failed');
    }
  }

  // ====================SalesNavigatorLead=========================================
  // ====================SalesNavigatorLead=========================================
  // ====================SalesNavigatorLead=========================================

  async findAllSalesNavigator(query: Record<string, any>, user: any) {
    const salesNavigatorData = await this.SalesNavigatorLead(
      'salesNavigatorLead',
      query,
      user,
    );

    return salesNavigatorData;
  }

  private async SalesNavigatorLead(
    model: 'salesNavigatorLead',
    query: Record<string, any>,
    user: any,
  ) {
    // Default pagination values
    const page = Number(query.page) > 0 ? Number(query.page) : 1;
    const limit = Number(query.limit) > 0 ? Number(query.limit) : 20;
    const skip = (page - 1) * limit;

    // FIX: Added deleted_at: null for consistency (soft delete)
    const where: any = { deleted_at: null, AND: [] };

    // 🔍 Global Search Logic
    if (query.q) {
      const search = String(query.q).trim();
      where.AND.push({
        OR: [
          { job_title: { contains: search, mode: 'insensitive' } },
          { company_domain: { contains: search, mode: 'insensitive' } },
          { url: { contains: search, mode: 'insensitive' } },
          { email_first: { contains: search, mode: 'insensitive' } },
          { email_second: { contains: search, mode: 'insensitive' } },
          { city: { contains: search, mode: 'insensitive' } },
          { location: { contains: search, mode: 'insensitive' } },
          { first_name: { contains: search, mode: 'insensitive' } },
          { last_name: { contains: search, mode: 'insensitive' } },
          { phone: { contains: search, mode: 'insensitive' } },
          { company_phone: { contains: search, mode: 'insensitive' } },
          { company_name: { contains: search, mode: 'insensitive' } },
          { company_id: { contains: search, mode: 'insensitive' } },
          { linkedin_id: { contains: search, mode: 'insensitive' } },
        ],
      });
    }

    // Handle query parameters for filtering
    for (const key of Object.keys(query)) {
      if (
        ['page', 'limit', 'name', 'sortBy', 'order', 'q', 'search'].includes(
          key,
        )
      )
        continue;

      const value = query[key];
      if (!value) continue;

      let values: string[] = [];
      try {
        values = Array.isArray(value) ? value : JSON.parse(value);
        if (!Array.isArray(values)) values = [String(value)];
      } catch {
        values = [String(value)];
      }

      where.AND.push({
        OR: values.map((v) => ({
          [key]: { contains: v, mode: 'insensitive' },
        })),
      });
    }

    if (where.AND.length === 0) {
      delete where.AND;
    }

    // Sorting setup
    const sortBy = query.sortBy || 'created_at';
    const order =
      query.order && ['asc', 'desc'].includes(query.order.toLowerCase())
        ? (query.order.toLowerCase() as Prisma.SortOrder)
        : Prisma.SortOrder.desc;
    const orderBy = { [sortBy]: order };

    // Model delegate for querying
    const delegate: {
      findMany: (args?: any) => Promise<any[]>;
      count: (args?: any) => Promise<number>;
    } = (this.prisma as any)[model];

    const data = await delegate.findMany({
      where,
      take: limit,
      skip,
      orderBy,
    });

    const total = await delegate.count({ where });

    return {
      success: true,
      message: 'Data fetched successfully',
      data,
      meta: {
        total,
        page,
        limit,
        pages: Math.ceil(total / limit),
      },
      access: 'authorized',
    };
  }

  async getJobTitle(search?: string) {
    try {
      const data = await this.prisma.salesNavigatorLead.findMany({
        where: {
          job_title: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, job_title: true },
        distinct: ['job_title'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'Job titles fetched successfully',
        data,
      };
    } catch {
      throw new BadRequestException('Job title fetch failed');
    }
  }

  async getCompanyDomain(search?: string) {
    try {
      const data = await this.prisma.salesNavigatorLead.findMany({
        where: {
          company_domain: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, company_domain: true },
        distinct: ['company_domain'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'Company domains fetched successfully',
        data,
      };
    } catch {
      throw new BadRequestException('Company domain fetch failed');
    }
  }

  async getLinkedinId(search?: string) {
    try {
      const data = await this.prisma.salesNavigatorLead.findMany({
        where: {
          url: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, url: true },
        distinct: ['url'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'LinkedIn IDs fetched successfully',
        data,
      };
    } catch {
      throw new BadRequestException('LinkedIn ID fetch failed');
    }
  }

  async getEmailFirst(search?: string) {
    try {
      const data = await this.prisma.salesNavigatorLead.findMany({
        where: {
          email_first: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, email_first: true },
        distinct: ['email_first'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'First emails fetched successfully',
        data,
      };
    } catch {
      throw new BadRequestException('First email fetch failed');
    }
  }

  async getEmailSecond(search?: string) {
    try {
      const data = await this.prisma.salesNavigatorLead.findMany({
        where: {
          email_second: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, email_second: true },
        distinct: ['email_second'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return {
        success: true,
        message: 'Second emails fetched successfully',
        data,
      };
    } catch {
      throw new BadRequestException('Second email fetch failed');
    }
  }

  async getCity2(search?: string) {
    try {
      const data = await this.prisma.salesNavigatorLead.findMany({
        where: {
          city: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, city: true },
        take: 10,
        distinct: ['city'],
        orderBy: { created_at: 'desc' },
      });
      return { success: true, message: 'Cities fetched successfully', data };
    } catch {
      throw new BadRequestException('City fetch failed');
    }
  }

  async getLocation(search?: string) {
    try {
      const data = await this.prisma.salesNavigatorLead.findMany({
        where: {
          location: { contains: search, mode: 'insensitive' },
        },
        select: { id: true, location: true },
        distinct: ['location'],
        take: 10,
        orderBy: { created_at: 'desc' },
      });
      return { success: true, message: 'Cities fetched successfully', data };
    } catch {
      throw new BadRequestException('City fetch failed');
    }
  }

  // DELETE ENDPOINT (Range-based Deletion)================================================================================
  async deleteLeadsByDateRange(
    type: 'SALES_NAVIGATOR' | 'ZOOMINFO' | 'APOLLO',
    startDateString: string,
    endDateString: string,
  ) {
    // 1. Date Validation and Conversion
    const startDate = new Date(startDateString);
    let endDate = new Date(endDateString);

    if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
      throw new BadRequestException(
        'Invalid date format. Please ensure both dates are valid ISO 8601 strings (e.g., YYYY-MM-DD).',
      );
    }

    if (startDate.getTime() >= endDate.getTime()) {
      throw new BadRequestException('Start date must be before end date.');
    }

    // If only YYYY-MM-DD is provided, ensure the end date is inclusive of the whole day.
    if (endDateString.length <= 10) {
      endDate.setHours(23, 59, 59, 999);
    }

    let modelDelegate: any;
    let modelName: string;

    switch (type) {
      case 'SALES_NAVIGATOR':
        modelDelegate = this.prisma.salesNavigatorLead;
        modelName = 'SalesNavigatorLead';
        break;
      case 'ZOOMINFO':
        modelDelegate = this.prisma.zoominfoLead;
        modelName = 'ZoominfoLead';
        break;
      case 'APOLLO':
        modelDelegate = this.prisma.apolloLead;
        modelName = 'ApolloLead';
        break;
      default:
        throw new BadRequestException('Invalid lead type for deletion');
    }

    // 2. Prisma Delete Operation
    const result = await modelDelegate.deleteMany({
      where: {
        created_at: {
          // gte: Greater Than or Equal (Inclusive start date)
          gte: startDate,
          // lte: Less Than or Equal (Inclusive end date)
          lte: endDate,
        },
      },
    });

    return {
      success: true,
      type: modelName,
      deletedCount: result.count,
      startDate: startDate.toISOString(),
      endDate: endDate.toISOString(),
      message: `${result.count} ${modelName} records created between ${startDateString} and ${endDateString} were successfully deleted.`,
    };
  }
}