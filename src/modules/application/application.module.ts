import { Module } from '@nestjs/common';
import { NotificationModule } from './notification/notification.module';
import { ContactModule } from './contact/contact.module';
import { FaqModule } from './faq/faq.module';
import { LeadDataModule } from './lead-data/lead-data.module';

@Module({
  imports: [NotificationModule, ContactModule, FaqModule, LeadDataModule],
})
export class ApplicationModule {}
