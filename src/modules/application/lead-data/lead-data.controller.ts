import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Post,
  Query,
  Req,
  UploadedFile,
  UseGuards,
  UseInterceptors
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { OptionalJwtAuthGuard } from 'src/modules/auth/guards/optional-jwt-auth.guard';
import { LeadDataService } from './lead-data.service';

@Controller('leads')
@UseGuards(OptionalJwtAuthGuard)
export class LeadDataController {
  constructor(private readonly LeadDataService: LeadDataService) {}

  // @Get()
  // @UsePipes(new ValidationPipe({ transform: true, whitelist: true }))
  // async getLeads(@Query() query: Record<string, any>, @Req() req: any) {
  //   const user = req.user || null; // user may be null if not logged in
  //   return this.LeadDataService.findAll(query, user);
  // }

@Post('import')
@UseInterceptors(FileInterceptor('file'))
async importLeads(
  @UploadedFile() file: Express.Multer.File,
  @Body('type') type: 'SALES_NAVIGATOR' | 'ZOOMINFO' | 'APOLLO',
  @Req() req,
) {
  try {
    if (!file) throw new BadRequestException('CSV file is required');
    if (!type) throw new BadRequestException('Lead type is required');

    const buffer = file.buffer.toString('utf-8');
    
    // userId can come from req.user.id if using auth middleware
    const userId = req.user?.sub;
    console.log('user id', req.user)
    if (!userId) throw new BadRequestException('User not found');

    return await this.LeadDataService.importCsv(buffer, type, userId);
  } catch (error) {
    // Return proper error
    if (error instanceof BadRequestException) {
      throw error;
    }
    console.error(error);
    throw new BadRequestException(error.message || 'Import failed');
  }
}


  // @Get('export')
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
  @Get('sales-navigator')
  async findAllSalesNavigator(@Query() query: Record<string, any>, @Req() req) {
    return this.LeadDataService.findAllSalesNavigator(query, req.user);
  }

  // 🔹 ZoomInfo - Find All
  @Get('zoominfo')
  async findAllZoominfo(@Query() query: Record<string, any>, @Req() req) {
    return this.LeadDataService.findAllZoominfo(query, req.user);
  }

  // 🔹 Apollo - Find All
  @Get('apollo')
  async findAllApollo(@Query() query: Record<string, any>, @Req() req) {
    return this.LeadDataService.findAllApollo(query, req.user);
  }

  // @Delete()
  // async deleteLeads(@Query('count') count?: string) {
  //   let parsedCount: number | undefined;

  //   if (count !== undefined) {
  //     parsedCount = Number(count);
  //     if (!Number.isInteger(parsedCount) || parsedCount <= 0) {
  //       throw new BadRequestException('Count must be a positive integer');
  //     }
  //   }

  //   const result = await this.LeadDataService.deleteLeads({
  //     count: parsedCount,
  //   });
  //   return {
  //     message: `Requested to delete ${result.requested} lead(s).`,
  //     deleted: result.deleted,
  //   };
  // }

  // filter api all

  @Get('job_titles')
  async getJobTitles() {
    return this.LeadDataService.getJobTitles();
  }
}
