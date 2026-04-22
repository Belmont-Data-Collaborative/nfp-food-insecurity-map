/**
 * Downloads all data files from S3 into data/ before Vercel serves the site.
 * Run via: node scripts/sync-data.mjs
 * Requires: @aws-sdk/client-s3 (installed in Vercel build step)
 */
import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";
import { createWriteStream, mkdirSync } from "fs";
import { pipeline } from "stream/promises";

const BUCKET = "bdc-projects";
const PREFIX = "nfp-food-insecurity-map/data/";
const OUTPUT_DIR = "data";

const client = new S3Client({ region: process.env.AWS_DEFAULT_REGION || "us-east-1" });

mkdirSync(OUTPUT_DIR, { recursive: true });

const list = await client.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: PREFIX }));
const objects = (list.Contents || []).filter(o => !o.Key.endsWith("/"));

if (objects.length === 0) {
  console.error("No files found under", PREFIX);
  process.exit(1);
}

console.log(`Syncing ${objects.length} files from s3://${BUCKET}/${PREFIX}`);

for (const obj of objects) {
  const filename = obj.Key.slice(PREFIX.length);
  const localPath = `${OUTPUT_DIR}/${filename}`;
  process.stdout.write(`  ${filename} ... `);
  const { Body } = await client.send(new GetObjectCommand({ Bucket: BUCKET, Key: obj.Key }));
  await pipeline(Body, createWriteStream(localPath));
  console.log("done");
}

console.log("Sync complete.");
