import { BadRequestException, Injectable } from '@nestjs/common';
import { Prisma } from '@prisma/client';
import { parse } from 'csv-parse/sync';
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

  async importFromCsv(
    buffer: Buffer,
    options: { type: string; user_id: string },
  ) {
    try {
      const { type, user_id } = options;
      const text = buffer.toString('utf-8');

      let records: any[];
      try {
        records = parse(text, {
          columns: true,
          skip_empty_lines: true,
          trim: true,
        });
      } catch {
        throw new BadRequestException('Invalid CSV format');
      }

      if (records.length === 0) throw new BadRequestException('CSV is empty');

      const headers = Object.keys(records[0]);
      const unknown = headers.filter((h) => !this.allowedFields.has(h));
      if (unknown.length)
        throw new BadRequestException(`Unknown columns: ${unknown.join(', ')}`);

      const chunkSize = 1000; // Adjust chunk size based on memory/performance
      const imported: any[] = [];
      const errors: { row: number; error: string; data?: any }[] = [];

      for (let i = 0; i < records.length; i += chunkSize) {
        const chunk = records.slice(i, i + chunkSize).map((row, index) => {
          // Add type & user_id
          row.type = type;
          row.user_id = user_id;

          // Numeric conversion
          if ('email_score' in row && row.email_score !== '') {
            const n = Number(row.email_score);
            row.email_score = Number.isNaN(n) ? null : n;
          }

          // Date conversion
          const dateFields = [
            'createdAt',
            'updated_at',
            'company_founded_at',
            'last_funding_stage',
            'Last Raised At',
          ];
          for (const key of dateFields) {
            if (row[key]) {
              const parsed = new Date(row[key]);
              row[key] = isNaN(parsed.getTime())
                ? undefined
                : parsed.toISOString();
            }
          }

          return row;
        });

        // Insert chunk in transaction
        try {
          const created = await this.prisma.leadData.createMany({
            data: chunk,
            skipDuplicates: true,
          });
          imported.push(created.count);
        } catch (err) {
          // Handle row-level errors individually
          for (let j = 0; j < chunk.length; j++) {
            try {
              await this.prisma.leadData.create({ data: chunk[j] });
              imported.push(1);
            } catch (rowErr) {
              errors.push({
                row: i + j + 1,
                error: rowErr.message || rowErr,
                data: chunk[j],
              });
            }
          }
        }
      }

      return {
        success: errors.length === 0,
        message:
          errors.length === 0
            ? `Imported ${imported.reduce((a, b) => a + b, 0)} records successfully.`
            : `Imported ${imported.reduce((a, b) => a + b, 0)} records with ${errors.length} errors.`,
        totalRows: records.length,
        imported: imported.reduce((a, b) => a + b, 0),
        errorsCount: errors.length,
        errors,
      };
    } catch (err) {
      throw new BadRequestException('Failed to parse CSV file');
    }
  }

  // EXPORT CSV: apply filters & return string
  async exportToCsv(query: any) {
    const { take, skip, where, orderBy } = this.buildPrismaQuery(query);
    const rows = await this.prisma.leadData.findMany({ where, orderBy });

    if (!rows.length) return { csv: '', count: 0 };

    // headers: union of all allowedFields present in rows; keep a stable order (allowedFields insertion order)
    const headers = Array.from(this.allowedFields).filter((h) =>
      rows.some((r) => r[h] !== undefined && r[h] !== null && r[h] !== ''),
    );

    // build CSV safely
    const escape = (val: any) => {
      if (val === null || val === undefined) return '';
      const s = String(val);
      if (s.includes('"') || s.includes(',') || s.includes('\n')) {
        return `"${s.replace(/"/g, '""')}"`;
      }
      return s;
    };

    const lines = [headers.join(',')];
    for (const r of rows) {
      const line = headers.map((h) => escape(r[h])).join(',');
      lines.push(line);
    }

    return { csv: lines.join('\n'), count: rows.length };
  }

  // GET with pagination, global search q and field filters
async findAll(query: Record<string, any>, user: any) {
  //  Pagination setup
  const page = Number(query.page) > 0 ? Number(query.page) : 1;
  const limit = Number(query.limit) > 0 ? Number(query.limit) : 20;
  const skip = (page - 1) * limit;

  // 2️⃣ Dynamic filter object
  const where: Prisma.LeadDataWhereInput = {};

  // 2a️⃣ Multi-name search (comma or space separated)
  if (query.name) {
    const names = query.name.split(/[,\s]+/).filter(Boolean);
    where.OR = names.map((n: string) => ({
      name: { contains: n, mode: 'insensitive' },
    }));
  }

  // 2b️⃣ Generic multi-value filters for all other fields
  for (const key of Object.keys(query)) {
    if (['page', 'limit', 'name', 'sortBy', 'order'].includes(key)) continue;

    const value = query[key];
    if (!value) continue;

    let values: string[] = [];

    // Try parse JSON array (for multi-value fields)
    try {
      values = Array.isArray(value) ? value : JSON.parse(value);
      if (!Array.isArray(values)) values = [String(value)];
    } catch {
      values = [String(value)];
    }

    // Ensure AND is an array (Prisma types allow AND to be an object or an array)
    if (!Array.isArray(where.AND)) {
      where.AND = where.AND ? [where.AND as any] : [];
    }
    (where.AND as any[]).push({
      OR: values.map((v) => ({
        [key]: { contains: v, mode: 'insensitive' },
      })),
    });
  }

  // 3️⃣ Sorting setup
  const sortBy = query.sortBy || 'createdAt';
  const order =
    query.order && ['asc', 'desc'].includes(query.order.toLowerCase())
      ? (query.order.toLowerCase() as Prisma.SortOrder)
      : Prisma.SortOrder.desc;

  const orderBy: Prisma.LeadDataOrderByWithRelationInput = {
    [sortBy]: order,
  };

  // 4️⃣ Guest restriction: only 20 total results, no pagination
  if (!user) {
    const data = await this.prisma.leadData.findMany({
      where,
      take: 20,
      orderBy,
    });

    return {
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

  // 5️⃣ Authorized user: full paginated data with count
  const [data, total] = await this.prisma.$transaction([
    this.prisma.leadData.findMany({
      where,
      take: limit,
      skip,
      orderBy,
    }),
    this.prisma.leadData.count({ where }),
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



  async deleteLeads(params: { count?: number }) {
    const MAX_DELETE = 5000; // max ekbar e delete kora jaabe
    const requestedCount = params.count ?? 1000; // default 1000
    const count = Math.min(requestedCount, MAX_DELETE);

    if (!Number.isInteger(count) || count <= 0) {
      throw new BadRequestException('Count must be a positive integer');
    }

    // Find oldest N records
    const items = await this.prisma.leadData.findMany({
      take: count,
      orderBy: { createdAt: 'asc' },
      select: { id: true },
    });

    if (items.length === 0) {
      return { requested: count, deleted: 0 };
    }

    const ids = items.map((i) => i.id);

    // Delete by ids
    const result = await this.prisma.leadData.deleteMany({
      where: { id: { in: ids } },
    });

    return { requested: count, deleted: result.count };
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

  async getJobTitles() {
    try {
      const titles = await this.prisma.leadData.findMany({
        select: {
          id: true,
          job_title: true,
        },
        take: 10,
        orderBy: { createdAt: 'desc' }, // newest first
      });

      return {
        success: true,
        message: 'All job title Fatch ',
        data: titles,
      };
    } catch (err) {
      throw new BadRequestException('Job title Fetch Faild' )
    }
  }

}
