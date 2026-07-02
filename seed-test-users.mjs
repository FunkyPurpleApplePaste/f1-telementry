import bcrypt from "bcryptjs";
import dotenv from "dotenv";
import { applicationDefault, cert, initializeApp } from "firebase-admin/app";
import { FieldValue, getFirestore } from "firebase-admin/firestore";

dotenv.config();

const BCRYPT_SALT_ROUNDS = Number(process.env.BCRYPT_SALT_ROUNDS || 12);

const seedUsers = [
  {
    username: "test",
    email: "test@test.com",
    password: "test1234",
    role: "user",
    isAdmin: false,
  },
  {
    username: "admin",
    email: "admin@admin.com",
    password: "admin1234",
    role: "admin",
    isAdmin: true,
  },
];

let credential;
if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  credential = cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT));
} else {
  credential = applicationDefault();
}

initializeApp({ credential });
const db = getFirestore();

function normalizeUsername(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function normalizeEmail(value) {
  return String(value ?? "").trim().toLowerCase();
}

async function upsertUser(seedUser) {
  const usernameLower = normalizeUsername(seedUser.username);
  const emailLower = normalizeEmail(seedUser.email);
  const passwordHash = await bcrypt.hash(seedUser.password, BCRYPT_SALT_ROUNDS);

  const usernameRef = db.collection("usernames").doc(usernameLower);
  const emailRef = db.collection("emails").doc(emailLower);

  await db.runTransaction(async (tx) => {
    const usernameSnap = await tx.get(usernameRef);
    const emailSnap = await tx.get(emailRef);

    const usernameUserId = usernameSnap.exists ? usernameSnap.data()?.userId : null;
    const emailUserId = emailSnap.exists ? emailSnap.data()?.userId : null;

    if (usernameUserId && emailUserId && usernameUserId !== emailUserId) {
      throw new Error(
        `Cannot seed ${seedUser.username}: username and email belong to different users.`
      );
    }

    const userRef = usernameUserId || emailUserId
      ? db.collection("users").doc(usernameUserId || emailUserId)
      : db.collection("users").doc();

    tx.set(
      userRef,
      {
        username: seedUser.username,
        usernameLower,
        email: seedUser.email,
        emailLower,
        passwordHash,
        role: seedUser.role,
        isAdmin: seedUser.isAdmin,
        updatedAt: FieldValue.serverTimestamp(),
        lastSeenAt: FieldValue.serverTimestamp(),
        createdAt: FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    tx.set(
      usernameRef,
      {
        userId: userRef.id,
        usernameLower,
        updatedAt: FieldValue.serverTimestamp(),
      },
      { merge: true }
    );

    tx.set(
      emailRef,
      {
        userId: userRef.id,
        emailLower,
        updatedAt: FieldValue.serverTimestamp(),
      },
      { merge: true }
    );
  });

  console.log(`Seeded ${seedUser.username} (${seedUser.email}) as ${seedUser.role}`);
}

for (const seedUser of seedUsers) {
  await upsertUser(seedUser);
}

console.log("Done.");
