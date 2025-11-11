import { BadRequestException, Injectable } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import * as Papa from 'papaparse';
import { PrismaService } from 'src/prisma/prisma.service';

@Injectable()
export class LeadDataService {
  // Allowed fields list - must match your Prisma model fields
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

  // IMPORT CSV

async importCsv(csvData: string, type: string, userId: string) {
  if (!userId) throw new BadRequestException('User not found');

  const expectedHeaders: string[] = [];
  let emailColumns: string[] = [];

  switch (type) {
    case 'SALES_NAVIGATOR':
      expectedHeaders.push(
        'first_name', 'last_name', 'job_title', 'email_first', 'email_second',
        'phone', 'company_phone', 'url', 'company_name', 'company_domain',
        'company_id', 'city', 'linkedin_id', 'created_date'
      );
      emailColumns = ['email_first', 'email_second'];
      break;

    case 'ZOOMINFO':
      expectedHeaders.push(
        'company_id','name', 'email', 'email_score','phone', 'work_phone', 'lead_location', 
        'lead_divison', 'lead_titles', 'seniority_level', 'skills', 
        'past_companies', 'company_name', 'company_size', 'company_phone_numbers',
        'company_location_text', 'company_type', 'company_industry', 
        'company_sector', 'company_facebook_page', 'revenue_range', 
        'ebitda_range', 'company_linkedin_page', 'company_sic_code', 
        'company_naics_code', 'company_size_key', 'linkedin_url', 
        'company_founded_at', 'company_website', 'company_products_services'
      );
      emailColumns = ['email'];
      break;

    case 'APOLLO':
      expectedHeaders.push(
        'first_name', 'last_name', 'title', 'company_name', 'company_name_for_emails',
        'email', 'email_status', 'primary_email_source', 'primary_email_verification_source',
        'email_confidence', 'primary_email_catch_all_status', 'primary_email_last_verified_at',
        'seniority', 'departments', 'contact_owner', 'work_direct_phone', 'home_phone',
        'mobile_phone', 'corporate_phone', 'other_phone', 'stage', 'lists', 
        'last_contacted', 'account_owner', 'employees', 'industry', 'keywords',
        'person_linkedin_url', 'website', 'company_uinkedin_url', 'facebook_url',
        'twitter_url', 'city', 'state', 'country', 'company_address', 'company_city',
        'company_state', 'company_country', 'company_phone', 'technologies',
        'annual_revenue', 'total_funding', 'latest_funding', 'latest_funding_amount',
        'last_raised_at', 'subsidiary_of', 'email_sent', 'email_open', 'email_bounced',
        'replied', 'demoed', 'number_of_retail_locations', 'apollo_contact_id',
        'apollo_account_id', 'secondary_email', 'secondary_email_source',
        'secondary_email_status', 'secondary_email_verification_source',
        'tertiary_email', 'tertiary_email_source', 'tertiary_email_status',
        'tertiary_email_verification_source'
      );
      emailColumns = ['email', 'secondary_email', 'tertiary_email'];
      break;

    default:
      throw new BadRequestException('Invalid lead type');
  }

  // Parse CSV
  const parsed = Papa.parse(csvData, { header: true, skipEmptyLines: false });

  if (parsed.errors.length) {
    return {
      success: false,
      message: 'CSV Parsing Error',
      errors: parsed.errors.map((e) => ({
        row: e.row,
        message: e.message,
      })),
    };
  }

  // Check headers
  const headers = parsed.meta.fields || [];
  const invalidHeaders = headers.filter((h) => !expectedHeaders.includes(h));
  if (invalidHeaders.length) {
    const details = invalidHeaders.map((h) => {
      const match = expectedHeaders.find((eh) => eh.toLowerCase() === h.toLowerCase());
      return match ? `${h} (did you mean '${match}'?)` : `${h} (unknown)`;
    });
    throw new BadRequestException(`Incorrect heading name: ${details.join(', ')}`);
  }

  // Row-level validation & cleaning
  const errorRows: any[] = [];
  const cleanedRows = parsed.data
    .map((row: any, index: number) => {
      const cleanRow: any = {};
      let rowHasError = false;
      const rowErrors: string[] = [];

      Object.keys(row).forEach((key) => {
        let value = String(row[key] || '').trim();

        // Convert "nan" or empty string to null
        if (value.toLowerCase() === 'nan' || value === '') value = null;

        // Remove extra quotes/brackets
        if (value) value = value.replace(/^(\[?['"]\]?)+|(['"]\]?)+$/g, '');

        // Validate email
        if (emailColumns.includes(key) && value && !value.includes('@')) {
          rowHasError = true;
          rowErrors.push(`Invalid email in column '${key}'`);
        }

        // Truncate string if too long for DB
        if (value && typeof value === 'string' && value.length > 1000) {
          value = value.substring(0, 1000);
        }

        cleanRow[key] = value;
      });

      const hasData = Object.values(cleanRow).some((v) => v !== null);
      if (!hasData) return null;

      if (rowHasError) {
        errorRows.push({ row: index + 2, errors: rowErrors, data: row });
        return null;
      }

      return { ...cleanRow, userId };
    })
    .filter((r) => r !== null);

  // Insert valid rows
  if (cleanedRows.length) {
    switch (type) {
      case 'SALES_NAVIGATOR':
        await this.prisma.salesNavigatorLead.createMany({ data: cleanedRows });
        break;
      case 'ZOOMINFO':
        await this.prisma.zoominfoLead.createMany({ data: cleanedRows });
        break;
      case 'APOLLO':
        await this.prisma.apolloLead.createMany({ data: cleanedRows });
        break;
    }
  }

  return {
    success: true,
    imported: cleanedRows.length,
    type,
    message: `${type} leads imported successfully.`,
    errors: errorRows.length ? errorRows : null,
  };
}



  // EXPORT CSV: apply filters & return string
  async exportToCsv(query: any) {
    // const { take, skip, where, orderBy } = this.buildPrismaQuery(query);
    // const rows = await this.prisma.leadData.findMany({ where, orderBy });
    // if (!rows.length) return { csv: '', count: 0 };
    // // headers: union of all allowedFields present in rows; keep a stable order (allowedFields insertion order)
    // const headers = Array.from(this.allowedFields).filter((h) =>
    //   rows.some((r) => r[h] !== undefined && r[h] !== null && r[h] !== ''),
    // );
    // // build CSV safely
    // const escape = (val: any) => {
    //   if (val === null || val === undefined) return '';
    //   const s = String(val);
    //   if (s.includes('"') || s.includes(',') || s.includes('\n')) {
    //     return `"${s.replace(/"/g, '""')}"`;
    //   }
    //   return s;
    // };
    // const lines = [headers.join(',')];
    // for (const r of rows) {
    //   const line = headers.map((h) => escape(r[h])).join(',');
    //   lines.push(line);
    // }
    // return { csv: lines.join('\n'), count: rows.length };
  }

  // ========================================================
  // 🔹 COMMON PAGINATION + FILTER LOGIC
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
        success:true,
        message:"Get All Data",
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

  // ========================================================
  // 🔹 MODEL-WISE FIND ALL FUNCTIONS
  // ========================================================

  async findAllSalesNavigator(query: Record<string, any>, user: any) {
    return this.dynamicFindAll('salesNavigatorLead', query, user);
  }

  async findAllZoominfo(query: Record<string, any>, user: any) {
    return this.dynamicFindAll('zoominfoLead', query, user);
  }

  async findAllApollo(query: Record<string, any>, user: any) {
    return this.dynamicFindAll('apolloLead', query, user);
  }

  // async deleteLeads(params: { count?: number }) {
  //   const MAX_DELETE = 5000; // max ekbar e delete kora jaabe
  //   const requestedCount = params.count ?? 1000; // default 1000
  //   const count = Math.min(requestedCount, MAX_DELETE);
  //   if (!Number.isInteger(count) || count <= 0) {
  //     throw new BadRequestException('Count must be a positive integer');
  //   }
  //   // Find oldest N records
  //   const items = await this.prisma.leadData.findMany({
  //     take: count,
  //     orderBy: { created_at: 'asc' },
  //     select: { id: true },
  //   });
  //   if (items.length === 0) {
  //     return { requested: count, deleted: 0 };
  //   }
  //   const ids = items.map((i) => i.id);
  //   // Delete by ids
  //   const result = await this.prisma.leadData.deleteMany({
  //     where: { id: { in: ids } },
  //   });
  //   return { requested: count, deleted: result.count };
  // }

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

  async getJobTitles() {
      try {
        const titles = await this.prisma.apolloLead.findMany({
          select: {
            id: true,
            title: true,
          },
          take: 10,
          orderBy: { created_at: 'desc' }, // newest first
        });
        return {
          success: true,
          message: 'All job title Fatch ',
          data: titles,
        };
      } catch (err) {
        throw new BadRequestException('Job title Fetch Faild');
      }
  }
}
