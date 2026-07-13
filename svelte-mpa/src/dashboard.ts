import './styles.css';
import { mount } from 'svelte';
import Dashboard from './pages/Dashboard.svelte';

mount(Dashboard, {
  target: document.getElementById('app') as HTMLElement
});
