-- CreateTable
CREATE TABLE "Transaction" (
    "id" SERIAL NOT NULL,
    "balance" INTEGER NOT NULL,
    "amount_id" INTEGER NOT NULL,

    CONSTRAINT "Transaction_pkey" PRIMARY KEY ("id")
);

-- AddForeignKey
ALTER TABLE "Transaction" ADD CONSTRAINT "Transaction_amount_id_fkey" FOREIGN KEY ("amount_id") REFERENCES "User"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
