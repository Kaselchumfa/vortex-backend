import { Module } from "@nestjs/common";
import { ArchivalService } from "./archival.service";
import { ArchivalJob } from "./archival.job";
import { ArchivalAdminController } from "./archival-admin.controller";
import { ArchivalRestoreJob } from "./archival-restore.job";

/**
 * ArchivalModule (#413).
 *
 * Registers the daily archival job, the ArchivalService, the admin restore
 * controller (#696) and the restore background job handler (#696).
 * Import in AppModule; the job is a no-op when ARCHIVAL_ENABLED=false.
 *
 * Requires PrismaModule (global), JobsModule (global) and @nestjs/schedule
 * (via ScheduleModule).
 */
@Module({
  controllers: [ArchivalAdminController],
  providers: [ArchivalService, ArchivalJob, ArchivalRestoreJob],
  exports: [ArchivalService],
})
export class ArchivalModule {}