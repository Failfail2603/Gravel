import { faker } from "@faker-js/faker";
import "./randomGenerator.js";

export function generateRandomEmail(): string {
  return faker.internet.email();
}

export function generateRandomAddress(): {
  street: string;
  zip: string;
  city: string;
} {
  return {
    street: faker.location.streetAddress(),
    zip: faker.location.zipCode(),
    city: faker.location.city(),
  };
}

export function generateRandomBirthday(): Date {
  return faker.date.birthdate({ min: 18, max: 80, mode: "age" });
}

export function generateRandomRole(): {
  startedAt: Date;
  role: string;
  contexts: string[];
} {
  const roles = [
    "user",
    "admin",
    "moderator",
    "editor",
    "viewer",
    "contributor",
  ];
  const contexts = [
    "sales",
    "marketing",
    "engineering",
    "support",
    "hr",
    "finance",
    "operations",
    "product",
  ];

  // Generate 1-4 random contexts
  const numContexts = faker.number.int({ min: 1, max: 4 });
  const selectedContexts = faker.helpers.arrayElements(contexts, numContexts);

  return {
    startedAt: faker.date.past({ years: 5 }),
    role: faker.helpers.arrayElement(roles),
    contexts: selectedContexts,
  };
}

export function generateRandomRoles(): {
  startedAt: Date;
  role: string;
  contexts: string[];
}[] {
  // Generate 1-3 roles per user
  const numRoles = faker.number.int({ min: 1, max: 3 });
  return Array.from({ length: numRoles }, () => generateRandomRole());
}

export function generateRandomDebitor(): number {
  return faker.number.int({ min: 100000, max: 999999 });
}

export function generateRandomTags(): string[][] {
  const possibleTags = [
    "premium",
    "verified",
    "new",
    "active",
    "vip",
    "trial",
    "enterprise",
    "partner",
    "beta",
    "legacy",
  ];

  // Generate 1-5 tag groups
  const numGroups = faker.number.int({ min: 1, max: 5 });
  const tagGroups: string[][] = [];

  for (let i = 0; i < numGroups; i++) {
    const numTags = faker.number.int({ min: 1, max: 3 });
    const tags = faker.helpers.arrayElements(possibleTags, numTags);
    tagGroups.push(tags);
  }

  return tagGroups;
}

export function generateRandomIban(): string {
  return faker.finance.iban();
}

export function generateRandomBic(): string {
  return faker.finance.bic();
}

export function generateRandomBankAccount(): {
  iban: string;
  bic: string;
  openedAt?: Date;
  active: boolean;
} {
  const bankAccount: any = {
    iban: generateRandomIban(),
    bic: generateRandomBic(),
    active: faker.datatype.boolean(),
  };

  // 60% chance to have openedAt
  if (Math.random() > 0.4) {
    bankAccount.openedAt = faker.date.past({ years: 10 });
  }

  return bankAccount;
}

export function generateRandomSepaEntry(): {
  name: string;
  startDate?: Date;
  endDate?: Date;
  price: number;
  bankAccount: {
    iban: string;
    bic: string;
    openedAt?: Date;
    active: boolean;
  };
} {
  const sepaEntry: any = {
    name: faker.company.name(),
    price: parseFloat(faker.finance.amount({ min: 10, max: 5000, dec: 2 })),
    bankAccount: generateRandomBankAccount(),
  };

  // 70% chance to have startDate
  if (Math.random() > 0.3) {
    sepaEntry.startDate = faker.date.past({ years: 3 });
  }

  // 40% chance to have endDate
  if (Math.random() > 0.6) {
    sepaEntry.endDate = faker.date.future({ years: 2 });
  }

  return sepaEntry;
}

export function generateRandomSepa(): {
  name: string;
  startDate?: Date;
  endDate?: Date;
  price: number;
  bankAccount: {
    iban: string;
    bic: string;
    openedAt?: Date;
    active: boolean;
  };
}[] {
  // Generate 1-4 SEPA entries
  const numEntries = faker.number.int({ min: 1, max: 4 });
  return Array.from({ length: numEntries }, () => generateRandomSepaEntry());
}

export function generateRandomUpdateFields(): object {
  const possibleFields = [
    "email",
    "archived",
    "roles",
    "birthday",
    "address",
    "debitor",
    "tags",
    "sepa",
  ];

  // Update 1-6 random fields
  const numFields = faker.number.int({ min: 1, max: 6 });
  const fieldsToUpdate = faker.helpers.arrayElements(possibleFields, numFields);

  const updateData: any = {};

  for (const field of fieldsToUpdate) {
    switch (field) {
      case "email":
        updateData.email = generateRandomEmail();
        break;
      case "archived":
        // Optional field: 30% chance to set to null, otherwise boolean
        if (Math.random() < 0.3) {
          updateData.archived = null;
        } else {
          updateData.archived = faker.datatype.boolean();
        }
        break;
      case "roles":
        updateData.roles = generateRandomRoles();
        break;
      case "birthday":
        // Optional field: 30% chance to set to null, otherwise generate date
        if (Math.random() < 0.3) {
          updateData.birthday = null;
        } else {
          updateData.birthday = generateRandomBirthday();
        }
        break;
      case "address":
        updateData.address = generateRandomAddress();
        break;
      case "debitor":
        updateData.debitor = generateRandomDebitor();
        break;
      case "tags":
        updateData.tags = generateRandomTags();
        break;
      case "sepa":
        updateData.sepa = generateRandomSepa();
        break;
    }
  }

  return updateData;
}
