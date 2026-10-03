import { InMemoryControlStore, InMemoryObjectStore } from '../../src/adapters/memory.js';
import { controlStoreContract, objectStoreContract } from './contract.js';

objectStoreContract('memory', () => new InMemoryObjectStore());
controlStoreContract('memory', () => new InMemoryControlStore());
