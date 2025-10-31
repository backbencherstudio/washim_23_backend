

import { Injectable, BadRequestException } from '@nestjs/common';
import { Readable } from 'stream';
import { parse } from 'csv-parse/sync';
import { PrismaService } from 'src/prisma/prisma.service';

@Injectable()
export class LeadDataService {
  // Allowed fields list - must match your Prisma model fields
  private allowedFields = new Set<string>([
    // system + sells + zoominfo + given JSON fields (use exact names you're storing)
    'id','type','user_id','first_name','last_name','job_title',
    'email_first','email_second','phone','company_phone','url','company_name',
    'company_id','company_domain','location','linkedin_id','created_at',
    'zoominfo_id','zoominfo_company_id','name','email','email_score','work_phone',
    'lead_location','lead_division','lead_title','decision_making_power',
    'seniority_level','linkedin_url','company_size','company_website',
    'company_location_text','company_type','company_function','company_sector',
    'company_industry','company_founded_at','company_funding_range','revenue_range',
    'ebitda_range','last_funding_stage','company_size_key',
    // plus the fields from the Apollo JSON - include them all as in your model:
    'title','company_name_for_emails','email_status','primary_email_source',
    'primary_email_verification_source','email_confidence','primary_email_catchall_status',
    'primary_email_last_verified_at','seniority','departments','contact_owner',
    'work_direct_phone','home_phone','mobile_phone','corporate_phone','other_phone',
    'stage','lists','last_contacted','account_owner','employees','industry','keywords',
    'person_linkedin_url','website','company_linkedin_url','facebook_url','twitter_url',
    'city','state','country','company_address','company_city','company_state','company_country',
    'technologies','annual_revenue','total_funding','latest_funding','latest_funding_amount',
    'last_raised_at','subsidiary_of','email_sent','email_open','email_bounced','replied',
    'demoed','number_of_retail_locations','apollo_contact_id','apollo_account_id',
    'secondary_email','secondary_email_source','secondary_email_status',
    'secondary_email_verification_source','tertiary_email','tertiary_email_source',
    'tertiary_email_status','tertiary_email_verification_source',
    'createdAt','updated_at'
  ]);

  constructor(private prisma: PrismaService) {}

  // IMPORT CSV

  async importFromCsv(buffer: Buffer) {
    const text = buffer.toString('utf-8');

    // Parse CSV
    let records: any[];
    try {
      records = parse(text, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
      });
    } catch (err) {
      throw new BadRequestException('Invalid CSV format');
    }

    if (records.length === 0) {
      throw new BadRequestException('CSV is empty');
    }

    // Validate headers
    const headers = Object.keys(records[0]);
    const unknown = headers.filter(h => !this.allowedFields.has(h));
    if (unknown.length) {
      throw new BadRequestException(`Unknown columns in CSV: ${unknown.join(', ')}`);
    }

    const imported = [];
    const errors: string[] = [];

    // Process records
    for (let i = 0; i < records.length; i++) {
      const row = records[i];

      // 1️⃣ Convert numeric fields
      if ('email_score' in row && row.email_score !== '') {
        const n = Number(row.email_score);
        row.email_score = Number.isNaN(n) ? null : n;
      }

      // 2️⃣ Convert date fields safely (auto format)
      const dateFields = ['createdAt', 'updated_at', 'company_founded_at', 'last_funding_stage', 'Last Raised At'];
      for (const key of dateFields) {
        if (row[key]) {
          const parsed = new Date(row[key]);
          if (!isNaN(parsed.getTime())) {
            row[key] = parsed.toISOString();
          } else {
            delete row[key]; // remove invalid date
          }
        }
      }

      // 3️⃣ Create record
      try {
        const created = await this.prisma.leadData.create({ data: row });
        imported.push(created);
      } catch (err) {
        errors.push(`Row ${i + 1} error: ${err.message || err}`);
      }
    }

    return {
      success: errors.length === 0,
      imported: imported.length,
      errors,
    };
  }

  // EXPORT CSV: apply filters & return string
  async exportToCsv(query: any) {
    const { take, skip, where, orderBy } = this.buildPrismaQuery(query);
    const rows = await this.prisma.leadData.findMany({ where, orderBy });

    if (!rows.length) return { csv: '', count: 0 };

    // headers: union of all allowedFields present in rows; keep a stable order (allowedFields insertion order)
    const headers = Array.from(this.allowedFields).filter(h =>
      rows.some(r => r[h] !== undefined && r[h] !== null && r[h] !== '')
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
      const line = headers.map(h => escape(r[h])).join(',');
      lines.push(line);
    }

    return { csv: lines.join('\n'), count: rows.length };
  }

  // GET with pagination, global search q and field filters
  async findAll(query: any) {
    const page = Number(query.page) > 0 ? Number(query.page) : 1;
    const limit = Number(query.limit) > 0 ? Number(query.limit) : 20;
    const skip = (page - 1) * limit;

    const { take, skip: _skip, where, orderBy } = this.buildPrismaQuery(query, { take: limit, skip });

    const [data, total] = await this.prisma.$transaction([
      this.prisma.leadData.findMany({ where, take: limit, skip, orderBy }),
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

  const ids = items.map(i => i.id);

  // Delete by ids
  const result = await this.prisma.leadData.deleteMany({
    where: { id: { in: ids } },
  });

  return { requested: count, deleted: result.count };
}



  // Build prisma where/orderBy from query
  private buildPrismaQuery(query: any, pagination?: { take: number; skip: number }) {
    const q = query.q?.trim();
    const where: any = {};
    const orderBy: any = {};

    // Field-specific filters: any query param that matches allowedFields
    for (const key of Object.keys(query)) {
      if (['page','limit','q','sortBy','order'].includes(key)) continue;
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
      const textFields = Array.from(this.allowedFields).filter(f =>
        ['first_name','last_name','job_title','email_first','email_second','company_name','url','location',
         'name','email','lead_title','linkedin_url','company_website','company_location_text','industry',
         'keywords','person_linkedin_url','website','company_linkedin_url','city','state','country','technologies'].includes(f)
      );

      where.OR = textFields.map(f => ({ [f]: { contains: q, mode: 'insensitive' } }));
    }

    // order
    if (query.sortBy && this.allowedFields.has(query.sortBy)) {
      const ord = query.order && query.order.toLowerCase() === 'asc' ? 'asc' : 'desc';
      orderBy[query.sortBy] = ord;
    } else {
      orderBy['createdAt'] = 'desc';
    }

    return { take: pagination?.take, skip: pagination?.skip, where, orderBy };
  }



}

