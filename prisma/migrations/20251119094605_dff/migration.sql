/*
  Warnings:

  - The `employees` column on the `ApolloLead` table would be dropped and recreated. This will lead to data loss if there is data in the column.

*/
-- AlterTable
ALTER TABLE "ApolloLead" DROP COLUMN "employees",
ADD COLUMN     "employees" INTEGER;
