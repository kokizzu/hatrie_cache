import './styles.css';
import { mount } from 'svelte';
import Catalog from './pages/Catalog.svelte';

mount(Catalog, { target: document.getElementById('app') as HTMLElement });
