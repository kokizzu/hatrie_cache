import './styles.css';
import { mount } from 'svelte';
import Admin from './pages/Admin.svelte';

mount(Admin, {
  target: document.getElementById('app') as HTMLElement
});
