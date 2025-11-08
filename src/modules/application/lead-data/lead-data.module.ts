import { Module } from '@nestjs/common';
import { JwtModule } from '@nestjs/jwt/dist/jwt.module';
import { LeadDataController } from './lead-data.controller';
import { LeadDataService } from './lead-data.service';

@Module({
   imports: [
    JwtModule.register({
      secret: process.env.JWT_SECRET || 'defaultSecret',
      signOptions: { expiresIn: '1h' },
    }),
  ],
  controllers: [LeadDataController],
  providers: [LeadDataService],
})
export class LeadDataModule {}
