import { PartialType } from '@nestjs/swagger';
import { CreateLeadDatumDto } from './create-lead-datum.dto';

export class UpdateLeadDatumDto extends PartialType(CreateLeadDatumDto) {}
