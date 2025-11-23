import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpStatus,
  Post,
  Query,
  Req,
  UploadedFile,
  UseGuards,
  UseInterceptors,
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { OptionalJwtAuthGuard } from 'src/modules/auth/guards/optional-jwt-auth.guard';
import { LeadDataService } from './lead-data.service';

@Controller('leads')
@UseGuards(OptionalJwtAuthGuard)
export class LeadDataController {
  constructor(private readonly LeadDataService: LeadDataService) {}

  private parseDate(dateStr: string, paramName = 'date'): Date {
    if (!dateStr) {
      throw new BadRequestException(`${paramName} query parameter is required`);
    }
    const date = new Date(dateStr);
    if (isNaN(date.getTime())) {
      throw new BadRequestException(`${paramName} is not a valid date`);
    }
    return date;
  }

  // ============================== import controller ==============================
@Post('import')
  @UseInterceptors(FileInterceptor('file')) // Max file size limit global config e thaka lagbe
  async importLeads(
    @UploadedFile() file: Express.Multer.File,
    @Body('type') type: string,
    @Req() req,
  ) {
    try {
      if (!file) throw new BadRequestException('CSV file is required');
      if (!type) throw new BadRequestException('Lead type is required');

      // userId extraction
      const userId = req.user?.sub;
      console.log('User ID requesting import:', userId);
      
      if (!userId) throw new BadRequestException('User not authenticated');
      
      return await this.LeadDataService.importCsv(file.buffer, type, userId);

    } catch (error) {
      console.error('Controller Error:', error);
      if (error instanceof BadRequestException) {
        throw error;
      }
      throw new BadRequestException(error.message || 'Import failed');
    }
  }


@Delete('sales-navigator/delete')
async deleteSalesNavigatorByDate(
  @Query('startDate') startDateStr: string,
  @Query('endDate') endDateStr: string,
  @Query('limit') limitStr?: string,
) {
  const startDate = this.parseDate(startDateStr, 'startDate');
  const endDate = this.parseDate(endDateStr, 'endDate');
  const limit = limitStr ? Number(limitStr) : undefined;

  return this.LeadDataService.deleteSalesNavigatorLeadsByDateRange(
    startDate,
    endDate,
    limit,
  );
}

@Delete('apollo/delete')
async deleteApolloByDate(
  @Query('startDate') startDateStr: string,
  @Query('endDate') endDateStr: string,
  @Query('limit') limitStr?: string,
) {
  const startDate = this.parseDate(startDateStr, 'startDate');
  const endDate = this.parseDate(endDateStr, 'endDate');
  const limit = limitStr ? Number(limitStr) : undefined;

  return this.LeadDataService.deleteApolloLeadsByDateRange(
    startDate,
    endDate,
    limit,
  );
}

@Delete('zoominfo/delete')
async deleteZoominfoByDate(
  @Query('startDate') startDateStr: string,
  @Query('endDate') endDateStr: string,
  @Query('limit') limitStr?: string,
) {
  const startDate = this.parseDate(startDateStr, 'startDate');
  const endDate = this.parseDate(endDateStr, 'endDate');
  const limit = limitStr ? Number(limitStr) : undefined;

  return this.LeadDataService.deleteZoominfoLeadsByDateRange(
    startDate,
    endDate,
    limit,
  );
}



  @Delete('all-DELETE')
    @HttpCode(HttpStatus.OK)
    async deleteAllData() {
        return await this.LeadDataService.deleteAllLeads();
    }

  @Get('export')
  // async exportCsv(@Query() query: Record<string, any>, @Res() res: Response) {
  //   const { csv, count } = await this.LeadDataService.exportToCsv(query);
  //   const filename = `leads_export_${Date.now()}.csv`;
  //   res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  //   res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
  //   res.status(HttpStatus.OK).send(csv);
  // }

  // ==============================
  // 📥 GET ENDPOINTS (Model-wise)
  // ==============================

  // 🔹 Sales Navigator - Find All

  // 🔹 ZoomInfo - Find All

  // 🔹 Apollo - Find All

  // filter api all
  // ====================Apollo=========================================
  // ====================Apollo=========================================
  // ====================Apollo=========================================

  

  @Get('job_titles22')
  async getJobTitles2(@Query('search') search: string) {
  console.log("hi")
  }


   @Get('apollo')
  async findAllApollo(
    @Query('q') q: string,
    @Query('email_status') email_status: string,
    @Query() query: Record<string, any>,
     @Req() req) {

    if (q) {
      query.q = q;
    }

    return this.LeadDataService.findAllApollo(query, req.user, email_status);
  }

 //f
  @Get('job_titless')
  async getJobTitles(@Query('search') search: string) {
    return this.LeadDataService.getJobTitles(search);
  }
 
 
 

  @Get('industry')
  async getIndustry(@Query('search') search: string) {
    return this.LeadDataService.getIndustry(search);
  }
  //f
  @Get('keyword')
  async getKeyword(@Query('search') search: string) {
    return this.LeadDataService.getKeyword(search);
  }

  @Get('technologies')
  async getTechnologies(@Query('search') search: string) {
    return this.LeadDataService.getTechnologies(search);
  }

  @Get('website')
  async getWebsite(@Query('search') search: string) {
    return this.LeadDataService.getWebsite(search);
  }
  // 
  @Get('company_linkedin')
  async getCompanyLinkedin(@Query('search') search: string) {
    return this.LeadDataService.getCompanyLinkedin(search);
  }

  @Get('country')
  async getCountry(@Query('search') search: string) {
    return this.LeadDataService.getCountry(search);
  }

  @Get('city')
  async getCity(@Query('search') search: string) {
    return this.LeadDataService.getCity(search);
  }

  @Get('state')
  async getState(@Query('search') search: string) {
    return this.LeadDataService.getState(search);
  }

  @Get('annual_revenue')
  async getAnnualRevenue(@Query('search') search: string) {
    return this.LeadDataService.getAnnualRevenue(search);
  }
  // 
  @Get('demoed')
  async getDemoed(@Query('search') search: string) {
    return this.LeadDataService.getDemoed(search);
  }

  // ====================ZoomInfo=========================================

  @Get('zoominfo')
  async findAllZoominfo(
    @Query('q') q: string,
    @Query() query: Record<string, any>, 
    @Req() req) {

    if (q) {
      query.q = q;
    }

    return this.LeadDataService.findAllZoominfo(query, req.user);
  }

  


  @Get('email')
  async getEmail(@Query('search') search: string) {
    return this.LeadDataService.getEmail(search);
  }

  @Get('lead_titles')
  async getLeadTitles(@Query('search') search: string) {
    return this.LeadDataService.getLeadTitles(search);
  }

  @Get('company_industry')
  async getCompanyIndustry(@Query('search') search: string) {
    return this.LeadDataService.getCompanyIndustry(search);
  }

  @Get('company_website')
  async getCompanyWebsite(@Query('search') search: string) {
    return this.LeadDataService.getCompanyWebsite(search);
  }

  @Get('revenue_range')
  async getRevenueRange(@Query('search') search: string) {
    return this.LeadDataService.getRevenueRange(search);
  }

  @Get('company_size')
  async getCompanySize(@Query('search') search: string) {
    return this.LeadDataService.getCompanySize(search);
  }

  @Get('company_location_text')
  async getCompanyLocationText(@Query('search') search: string) {
    return this.LeadDataService.getCompanyLocationText(search);
  }

  @Get('company_size_key')
  async getCompanySizeKey(@Query('search') search: string) {
    return this.LeadDataService.getCompanySizeKey(search);
  }

  @Get('skills')
  async getkeyword(@Query('search') search: string) {
    return this.LeadDataService.getSkill(search);
  }

  // ====================SalesNavigatorLead=========================================
  // ====================SalesNavigatorLead=========================================
  // ====================SalesNavigatorLead=========================================

  @Get('sales-navigator')
  async findAllSalesNavigator(
    @Query('q') q:string,
    @Query() query: Record<string, any>,
    @Req() req) {
 
    if (q) {
      query.q = q;
    }

    return this.LeadDataService.findAllSalesNavigator(query, req.user);
  }

  @Get('job_title')
  async getJobTitle(@Query('search') search: string) {
    return this.LeadDataService.getJobTitle(search);
  }

  @Get('company_domain')
  async getCompanyDomain(@Query('search') search: string) {
    return this.LeadDataService.getCompanyDomain(search);
  }
  //

  @Get('linkedin_id')
  async getLinkedinId(@Query('search') search: string) {
    return this.LeadDataService.getLinkedinId(search);
  }

  @Get('email_first')
  async getEmailFirst(@Query('search') search: string) {
    return this.LeadDataService.getEmailFirst(search);
  }

  @Get('email_second')
  async getEmailSecond(@Query('search') search: string) {
    return this.LeadDataService.getEmailSecond(search);
  }

  @Get('city2')
  async getCity2(@Query('search') search: string) {
    return this.LeadDataService.getCity2(search);
  }

  @Get('location')
  async getlocation(@Query('search') search: string) {
    return this.LeadDataService.getLocation(search);
  }
 
  // search====================================


}
