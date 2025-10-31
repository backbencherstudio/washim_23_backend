import { Module } from '@nestjs/common';
import { LeadDataService } from './lead-data.service';
import { LeadDataController } from './lead-data.controller';

@Module({
  controllers: [LeadDataController],
  providers: [LeadDataService],
})
export class LeadDataModule {}
