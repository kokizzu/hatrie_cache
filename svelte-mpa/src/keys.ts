import './styles.css';
import { mount } from 'svelte';
import Keys from './pages/Keys.svelte';

mount(Keys, {
  target: document.getElementById('app') as HTMLElement
});
