import { PrismaClient } from '@prisma/client';

// Ensure a single PrismaClient instance across the app (avoids pool exhaustion)
const globalForPrisma = global as unknown as { prisma?: PrismaClient };

export const prisma =
  globalForPrisma.prisma ||
  new PrismaClient({
    // Keep logs minimal for production; adjust as needed
    log: process.env.NODE_ENV === 'production' ? [] : ['warn', 'error'],
  });

if (!globalForPrisma.prisma) {
  globalForPrisma.prisma = prisma;
}
