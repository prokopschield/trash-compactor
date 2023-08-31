#!/usr/bin/env node

import { homedir } from "os";
import path from "path";

import { compact } from ".";

compact(path.resolve(homedir(), "trash.d"), false);
