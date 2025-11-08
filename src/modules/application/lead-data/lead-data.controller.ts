import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
  UsePipes,
  ValidationPipe,
  Query,
  UseInterceptors,
  UploadedFile,
  BadRequestException,
  Res,
  HttpStatus,
  UseGuards,
  Req,
} from '@nestjs/common';
import { Response } from 'express';
import { LeadDataService } from './lead-data.service';
import { CreateLeadDatumDto } from './dto/create-lead-datum.dto';
import { UpdateLeadDatumDto } from './dto/update-lead-datum.dto';
import { FileInterceptor } from '@nestjs/platform-express';
import { JwtAuthGuard } from 'src/modules/auth/guards/jwt-auth.guard';
import { OptionalJwtAuthGuard } from 'src/modules/auth/guards/optional-jwt-auth.guard';
import { Role } from 'src/common/guard/role/role.enum';

@Controller('leads')
export class LeadDataController {
  constructor(private readonly LeadDataService: LeadDataService) {}

@Get()
@UseGuards(OptionalJwtAuthGuard) 
@UsePipes(new ValidationPipe({ transform: true, whitelist: true }))
async getLeads(@Query() query: Record<string, any>, @Req() req: any) {
  const user = req.user || null; // user may be null if not logged in
  return this.LeadDataService.findAll(query, user);
}


  @Post('import')
  @UseGuards(JwtAuthGuard) 
  @UseInterceptors(FileInterceptor('file'))
  async importCsv(
    @UploadedFile() file: Express.Multer.File,
    @Body('type') type: string,
    @Req() req: any,
  ) {
    if (!file) throw new BadRequestException('CSV file is required');
    if (!file.originalname.match(/\.csv$/i))
      throw new BadRequestException('Only .csv files are accepted');
    if (!type) throw new BadRequestException('type is required');

    const user_id = req.user.id; // Extract user_id from JWT

    const result = await this.LeadDataService.importFromCsv(file.buffer, {
      type,
      user_id,
    });
    return result;
  }

  @Get('export')
  async exportCsv(@Query() query: Record<string, any>, @Res() res: Response) {
    const { csv, count } = await this.LeadDataService.exportToCsv(query);
    const filename = `leads_export_${Date.now()}.csv`;
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.status(HttpStatus.OK).send(csv);
  }

  @Delete()
  async deleteLeads(@Query('count') count?: string) {
    let parsedCount: number | undefined;

    if (count !== undefined) {
      parsedCount = Number(count);
      if (!Number.isInteger(parsedCount) || parsedCount <= 0) {
        throw new BadRequestException('Count must be a positive integer');
      }
    }

    const result = await this.LeadDataService.deleteLeads({
      count: parsedCount,
    });
    return {
      message: `Requested to delete ${result.requested} lead(s).`,
      deleted: result.deleted,
    };
  }


  // filter api all

  @Get('job_titles')
  async getJobTitles() {
    return this.LeadDataService.getJobTitles();
  }
}
