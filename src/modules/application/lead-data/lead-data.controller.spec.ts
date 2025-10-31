import { Test, TestingModule } from '@nestjs/testing';
import { LeadDataController } from './lead-data.controller';
import { LeadDataService } from './lead-data.service';

describe('LeadDataController', () => {
  let controller: LeadDataController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [LeadDataController],
      providers: [LeadDataService],
    }).compile();

    controller = module.get<LeadDataController>(LeadDataController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
