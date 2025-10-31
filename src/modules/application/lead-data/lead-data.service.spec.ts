import { Test, TestingModule } from '@nestjs/testing';
import { LeadDataService } from './lead-data.service';

describe('LeadDataService', () => {
  let service: LeadDataService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [LeadDataService],
    }).compile();

    service = module.get<LeadDataService>(LeadDataService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
